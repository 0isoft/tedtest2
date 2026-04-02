import time
from app.core.queue import dequeue_notice
from app.core.database import SessionLocal
from app.models.raw_notice import RawNotice
from app.models.opportunity import Opportunity
from app.models.opportunity_update import OpportunityUpdate
from app.models.requirement import Requirement
from datetime import datetime
from app.core.queue import enqueue_notice
import httpx
from lxml import etree
import re

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
    

def parse_xml_to_opportunity(xml_bytes):
    try:
        root = etree.fromstring(xml_bytes)
    except Exception as e:
        print(f"[XML PARSE ERROR] {e}")
        return None

    def gettext(path):
        return root.findtext(path, namespaces=NS)

    def getnode(path):
        return root.find(path, namespaces=NS)

    # --- BASIC ---
    title = gettext(".//cbc:Name")
    description = gettext(".//cbc:Description")

    country = gettext(".//cbc:IdentificationCode")

    # --- VALUE ---
    value_node = getnode(".//cbc:TotalAmount")
    if value_node is None:
        value_node = getnode(".//efbc:FrameworkMaximumAmount")

    estimated_value = float(value_node.text) if value_node is not None else None
    currency = value_node.get("currencyID") if value_node is not None else None

    # --- DEADLINE ---
    deadline = gettext(".//cbc:EndDate")

    # --- LOTS ---
    lots = []
    lot_nodes = root.findall(".//cac:ProcurementProjectLot", namespaces=NS)

    for lot in lot_nodes:
        lot_id = lot.findtext(".//cbc:ID", namespaces=NS)
        lot_name = lot.findtext(".//cbc:Name", namespaces=NS)

        lot_value = lot.findtext(".//efbc:FrameworkMaximumAmount", namespaces=NS)

        lots.append({
            "id": lot_id,
            "name": lot_name,
            "value": float(lot_value) if lot_value else None
        })

    # --- REQUIREMENTS ---
    requirements = []

    criteria_nodes = root.findall(".//cac:AwardingCriterion", namespaces=NS)

    for c in criteria_nodes:
        desc = c.findtext(".//cbc:Description", namespaces=NS)
        expr = c.findtext(".//cbc:CalculationExpression", namespaces=NS)

        if desc or expr:
            requirements.append({
                "text": desc or expr,
                "type": "award_criteria"
            })

    return {
        "title": title,
        "description": description,
        "buyer_country": country,
        "estimated_value": estimated_value,
        "currency": currency,
        "deadline": deadline,
        "lots": lots,
        "requirements": requirements
    }


def process_notice(db, notice_id, xml_bucket):
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

    if not xml_bytes:
        print("XML fetch failed, should retry later")
        enqueue_notice(str(notice.id))#this will be a landmine since sth will always stay in the queue
        return

    parsed = parse_xml_to_opportunity(xml_bytes)
    if not parsed:
        enqueue_notice(str(notice.id))
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

    while True:
        notice_id = dequeue_notice()

        if not notice_id:
            time.sleep(1)
            continue
        print(f"Dequeued: {notice_id}", flush=True)
        db = SessionLocal()

        try:
            process_notice(db, notice_id, xml_bucket)
        finally:
            db.close()

if __name__ == "__main__":
    worker_loop()