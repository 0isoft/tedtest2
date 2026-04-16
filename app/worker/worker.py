import time
from app.core.database import SessionLocal
from app.models.raw_notice import RawNotice
from app.models.opportunity import Opportunity
from app.models.opportunity_update import OpportunityUpdate
from app.models.requirement import Requirement
from app.models.lot import Lot
from datetime import datetime
import httpx
from lxml import etree
from app.core.queue import RedisQueue
from app.core.config import settings

class TokenBucket:
    def __init__(self, rate, capacity):
        """
        rate: tokens per second
        capacity: max burst
        """
        self.rate = rate
        self.capacity = capacity
        self.tokens = capacity
        self.last_refill = time.time()

    def wait_for_token(self):
        #todo: use celery beat to schedule  this
        while True:
            now = time.time()
            elapsed = now - self.last_refill

            # refill tokens
            self.tokens = min(
                self.capacity,
                self.tokens + elapsed * self.rate
            )
            self.last_refill = now

            if self.tokens >= 1:
                self.tokens -= 1
                return

            sleep_time = (1 - self.tokens) / self.rate
            time.sleep(sleep_time)


NS = {
    "cbc": "urn:oasis:names:specification:ubl:schema:xsd:CommonBasicComponents-2",
    "cac": "urn:oasis:names:specification:ubl:schema:xsd:CommonAggregateComponents-2",
    "efac": "http://data.europa.eu/p27/eforms-ubl-extension-aggregate-components/1",
    "efbc": "http://data.europa.eu/p27/eforms-ubl-extension-basic-components/1",
}
REQUIRED_FIELDS = ["title", "description"]

OPTIONAL_FIELDS = [
    "buyer_country",
    "estimated_value",
    "currency",
    "deadline"
]

def build_org_map(root):
    org_map = {}

    for org in root.findall(".//efac:Organization", namespaces=NS):
        org_id = org.findtext(".//cbc:ID", namespaces=NS)
        name = org.findtext(".//cac:PartyName/cbc:Name", namespaces=NS)

        if org_id:
            org_map[org_id] = name

    return org_map


def extract_buyer_name(root, org_map):
    org_id = root.findtext(
        ".//cac:ContractingParty//cac:PartyIdentification/cbc:ID",
        namespaces=NS
    )

    if org_id:
        return org_map.get(org_id)

    return None


def extract_publication_date(root):
    paths = [
        ".//efac:Publication/efbc:PublicationDate",
        ".//cbc:IssueDate"
    ]

    for p in paths:
        val = root.findtext(p, namespaces=NS)
        if val:
            return val

    return None

def extract_documents_url(root):
    return root.findtext(
        ".//cac:CallForTendersDocumentReference//cbc:URI",
        namespaces=NS
    )

def extract_deadline(root):
    return root.findtext(
        ".//cac:TenderSubmissionDeadlinePeriod/cbc:EndDate",
        namespaces=NS
    )



def parse_date(value: str):
    if not value:
        return None

    value = value.strip()

    # Handle date with trailing Z
    if value.endswith("Z") and "T" not in value:
        value = value[:-1]

    # If it's date-only with timezone like 2026-03-25+01:00, strip timezone
    if "T" not in value and len(value) > 10:
        value = value[:10]

    try:
        return datetime.fromisoformat(value)
    except Exception:
        return None

def extract_xml_url(raw: dict) -> str:
    links = raw.get("links", {})
    xml_links = links.get("xml", {})

    if not xml_links:
        return None

    # MUL = multilingual (best choice)
    if "MUL" in xml_links:
        return xml_links["MUL"]

    # fallback
    return list(xml_links.values())[0]

# fetch xml call should be rate limited 
def fetch_xml(xml_bucket, url: str):
    try:
        xml_bucket.wait_for_token()

        r = httpx.get(url, timeout=20)

        if r.status_code == 429:
            retry_after = int(r.headers.get("Retry-After", 5))
            print(f"429 XML → sleeping {retry_after}s")
            time.sleep(retry_after)
            return None

        r.raise_for_status()
        return r.content

    except Exception as e:
        print(f"[XML FETCH ERROR] {e}")
        return None
    
#########################################

def extract_title(root):
    paths = [
        ".//cac:ProcurementProject/cbc:Name",
        ".//cac:ProcurementProjectLot//cbc:Name",
    ]

    for p in paths:
        val = root.findtext(p, namespaces=NS)
        if val:
            return val

    return None    

def extract_description(root):
    paths = [
        ".//cac:ProcurementProject/cbc:Description",
        ".//cac:ProcurementProjectLot//cbc:Description",
    ]

    for p in paths:
        val = root.findtext(p, namespaces=NS)
        if val:
            return val

    return None

def extract_value(root):
    paths = [
        ".//cbc:EstimatedOverallContractAmount",
        ".//cbc:TotalAmount",
        ".//efbc:FrameworkMaximumAmount"
    ]

    for p in paths:
        node = root.find(p, namespaces=NS)
        if node is not None and node.text:
            try:
                value = float(node.text.strip())
                currency = node.get("currencyID")
                return value, currency
            except:
                continue

    return None, None




def extract_award_criteria(node):
    results = []

    for c in node.findall(".//cac:AwardingCriterion", namespaces=NS):
        desc = c.findtext(".//cbc:Description", namespaces=NS)
        if desc and desc.strip():
            results.append({
                "text": desc.strip(),
                "type": "award"
            })

    return results

def extract_global_requirements(root):
    results = []

    top_tendering_terms = root.findall("./cac:TenderingTerms", namespaces=NS)
    for tt in top_tendering_terms:
        results.extend(extract_selection_criteria(tt))

    top_awarding_terms = root.findall("./cac:TenderingTerms/cac:AwardingTerms", namespaces=NS)
    for at in top_awarding_terms:
        results.extend(extract_award_criteria(at))

    return results


def extract_country(root):
    paths = [
        ".//cac:ContractingParty//cac:PostalAddress/cac:Country/cbc:IdentificationCode",
        ".//cac:ProcurementProject//cac:RealizedLocation/cac:Address/cac:Country/cbc:IdentificationCode",
    ]

    for p in paths:
        val = root.findtext(p, namespaces=NS)
        if val:
            return val

    return None

def extract_selection_criteria(node):
    results = []

    for sc in node.findall(".//efac:SelectionCriteria", namespaces=NS):
        desc = sc.findtext(".//cbc:Description", namespaces=NS)
        if desc and desc.strip():
            results.append({
                "text": desc.strip(),
                "type": "eligibility"
            })

    return results

def extract_lots(root):
    lots = []
    lot_nodes = root.findall(".//cac:ProcurementProjectLot", namespaces=NS)

    for lot in lot_nodes:
        lot_id = lot.findtext("./cbc:ID", namespaces=NS)
        lot_name = lot.findtext(".//cac:ProcurementProject/cbc:Name", namespaces=NS)
        lot_desc = lot.findtext(".//cac:ProcurementProject/cbc:Description", namespaces=NS)

        value = None
        currency = None

        value_node = lot.find(
            ".//efbc:FrameworkMaximumAmount",
            namespaces=NS
        )
        if value_node is not None and value_node.text:
            try:
                value = float(value_node.text.strip())
                currency = value_node.get("currencyID")
            except Exception:
                pass

        requirements = (extract_selection_criteria(lot) + extract_award_criteria(lot))

        lots.append({
            "id": lot_id,
            "name": lot_name,
            "description": lot_desc,
            "estimated_value": value,
            "currency": currency,
            "requirements": requirements
        })

    return lots
    
def parse_xml(xml_bytes):
    return etree.fromstring(xml_bytes)

def extract_opportunity(root):
    value, currency = extract_value(root)
    org_map=build_org_map(root)
    return {
        "title": extract_title(root),
        "description": extract_description(root),
        "buyer_name": extract_buyer_name(root, org_map),
        "buyer_country": extract_country(root),
        "publication_date": extract_publication_date(root),
        "documents_url": extract_documents_url(root),
        "estimated_value": value,
        "currency": currency,
        "deadline": extract_deadline(root),
        "lots": extract_lots(root),
        "requirements": extract_global_requirements(root)
    }
    

def process_notice(queue, db, notice_id, xml_bucket):
    notice = db.query(RawNotice).filter_by(id=notice_id).first()

    if not notice or notice.processed:
        return

    raw = notice.raw_payload
    external_id = notice.external_id

    print(f"Processing {external_id}", flush=True)

    xml_url = extract_xml_url(raw)

    if not xml_url:
        print("No XML, fallback to old parsing")
        return

    xml_bytes = fetch_xml(xml_bucket, xml_url)
    print(xml_bytes[:500])

    maxretries=5
    if not xml_bytes:
        print("XML fetch failed, should retry later")
        #queue.enqueue(str(notice.id))#this will be a landmine since sth will always stay in the queue
        #fixed below
        notice.retry_count += 1
        notice.last_error = "xml_fetch_failed"
        if notice.retry_count < maxretries:
            queue.enqueue(str(notice.id))
        else:
            print(f"[DEAD LETTER] Notice {notice.id} failed permanently")

        db.commit()
        return

    root = parse_xml(xml_bytes)
    parsed = extract_opportunity(root)
    if not parsed:
        queue.enqueue(str(notice.id))
        return

    
    #  UPSERT Opportunity
    opportunity = db.query(Opportunity).filter_by(
        external_id=external_id
    ).first()

    if not opportunity:
        opportunity = Opportunity(
            external_source="TED",
            external_id=external_id,
            buyer_name=parsed["buyer_name"],
            buyer_country=parsed["buyer_country"],
            publication_date=parse_date(parsed["publication_date"]),
            deadline=parse_date(parsed["deadline"]),
            title=parsed["title"],
            description=parsed["description"],
            estimated_value=parsed["estimated_value"],
            currency=parsed["currency"],
            source_url=xml_url,
            documents_url=parsed["documents_url"],
            raw_payload=raw,
            status="new"
        )

        db.add(opportunity)
        db.flush()

        for r in parsed["requirements"]:
            db.add(Requirement(
                opportunity_id=opportunity.id,
                lot_id=None,
                text=r["text"],
                type=r["type"],
                source="ted_xml"
            ))
        for lot_data in parsed["lots"]:
            lot = Lot(
                opportunity_id=opportunity.id,
                external_lot_id=lot_data["id"],
                title=lot_data["name"],
                description=lot_data["description"],
                estimated_value=lot_data["estimated_value"],
                currency=lot_data["currency"],
            )
            db.add(lot)
            db.flush()
            for r in lot_data["requirements"]:
                db.add(Requirement(
                opportunity_id=None,
                lot_id=lot.id,
                text=r["text"],
                type=r["type"],
                source="ted_xml"
            ))
    else:
        db.add(OpportunityUpdate(
            opportunity_id=opportunity.id,
            update_type="xml_update",
            description=(parsed["description"] or "")[:500],
            source_url=xml_url,
            raw_payload=raw,
        ))

    notice.processed = True
    db.commit()



def worker_loop():
    print("Worker started...")
    xml_bucket = TokenBucket(rate=1/2, capacity=2)  
    queue = RedisQueue(settings.REDIS_URL, "raw_notice_queue")
    while True:
        notice_id = queue.dequeue()

        if not notice_id:
            time.sleep(1)
            continue
        print(f"Dequeued: {notice_id}", flush=True)
        db = SessionLocal()

        try:
            process_notice(queue, db, notice_id, xml_bucket)
        finally:
            db.close()

if __name__ == "__main__":
    worker_loop()