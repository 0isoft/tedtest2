import time
from app.core.database import SessionLocal
from app.models.raw_notice import RawNotice
from app.models.opportunity import Opportunity
from app.models.opportunity_update import OpportunityUpdate
from app.models.requirement import Requirement
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

def parse_date(value: str): 
    if not value: return None 
    try: 
        return datetime.fromisoformat(value) 
    except: return None

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

def extract_deadline(root):
    paths = [
        ".//cac:TenderSubmissionDeadlinePeriod/cbc:EndDate",
        ".//cbc:EndDate"
    ]

    for p in paths:
        val = root.findtext(p, namespaces=NS)
        if val:
            return val

    return None

def extract_selection_criteria(root):
    results = []

    for sc in root.findall(".//efac:SelectionCriteria", namespaces=NS):
        desc = sc.findtext(".//cbc:Description", namespaces=NS)
        if desc:
            results.append({
                "text": desc,
                "type": "eligibility"
            })

    return results

def extract_award_criteria(root):
    results = []

    for c in root.findall(".//cac:AwardingCriterion", namespaces=NS):
        desc = c.findtext(".//cbc:Description", namespaces=NS)

        if desc and desc.strip():
            results.append({
                "text": desc.strip(),
                "type": "award"
            })

    return results

def extract_requirements(root):

    return (
        extract_selection_criteria(root) +   
        extract_award_criteria(root)         
    )

def extract_country(root):
    paths = [
        # BEST: contracting authority
        ".//cac:ContractingParty//cbc:IdentificationCode",

        # fallback: project location
        ".//cac:ProcurementProject//cbc:IdentificationCode",
    ]

    for p in paths:
        val = root.findtext(p, namespaces=NS)
        if val:
            return val

    return None


def extract_lots(root):
    lots = []

    lot_nodes = root.findall(".//cac:ProcurementProjectLot", namespaces=NS)

    for lot in lot_nodes:
        lot_id = lot.findtext(".//cbc:ID", namespaces=NS)
        lot_name = lot.findtext(".//cbc:Name", namespaces=NS)
        lot_desc = lot.findtext(".//cbc:Description", namespaces=NS)

        # value (optional per lot)
        value_node = lot.find(".//cbc:EstimatedOverallContractAmount", namespaces=NS)
        lot_value = None
        currency = None

        if value_node is not None and value_node.text:
            try:
                lot_value = float(value_node.text)
                currency = value_node.get("currencyID")
            except:
                pass

        lots.append({
            "id": lot_id,
            "name": lot_name,
            "description": lot_desc,
            "value": lot_value,
            "currency": currency
        })

    return lots
    
def parse_xml(xml_bytes):
    return etree.fromstring(xml_bytes)

def extract_opportunity(root):
    value, currency = extract_value(root)

    return {
        "title": extract_title(root),
        "description": extract_description(root),
        "buyer_country": extract_country(root),
        "estimated_value": value,
        "currency": currency,
        "deadline": extract_deadline(root),
        "lots": extract_lots(root),
        "requirements": extract_requirements(root)
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
            buyer_country=parsed["buyer_country"],
            title=parsed["title"],
            description=parsed["description"],
            estimated_value=parsed["estimated_value"],
            currency=parsed["currency"],
            deadline=parse_date(parsed["deadline"]),
            source_url=xml_url,
            raw_payload=raw,
            status="new"
        )

        db.add(opportunity)
        db.flush()

        for r in parsed["requirements"]:
            db.add(Requirement(
                opportunity_id=opportunity.id,
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