# GreenGrid
GreenGrid Solutions is running a critical Pilot Program for 100 homes. The goal is to test a modern "Smart Grid" system where customers pay different rates based on when they use power.
The pilot has two major goals:
1. Dynamic Billing: Charge higher rates during "Peak Hours" (6 PM - 9 PM) to encourage
conservation.
2. Data Privacy: The legal team demands that no customer emails be stored in the analytics layer
(Silver/Gold) to comply with new privacy laws.
Situation Analysis & The Fact Pattern
The project is currently blocked. The sensors are sending messy data (negatives and massive spikes). The
billing team is trying to calculate "Peak Hour" costs in Excel, but they are making mistakes matching the
timestamps. Furthermore, the Compliance Officer just found a spreadsheet containing raw customer emails
on a shared drive, which is a major violation.
Your manager has scrapped the Excel process. She wants a Fabric Notebook solution that handles the
complex math, cleans the bad data, and automatically secures the PII (Personally Identifiable Information).
Central Decision
• Head of Product: "I need to prove this billing model works. If a customer uses power at 7 PM, they pay
double. If they use it at 2 PM, they pay the standard rate. But I also need you to filter out these
'Phantom Spikes' I can't bill someone $5,000 for a toaster glitch."
• Chief Legal Officer: "I don't care about the billing as much as the privacy. You cannot save raw email
addresses in the Silver analytics table. Hash them or mask them. If I see a raw email in the final report,
we shut the pilot down."
Exhibits
Exhibit 1: meter_readings.json (The Source Data) Note: Contains timestamps, usage, and status.
[

CloudMesh360 | Center of Excellence for Microsoft Fabric

This case study is part of Microsoft Boot Camp #2 led by Surya Kumar (CoE) in Dec 2025 Page 2 of 3
{ "meter_id": "M-101", "timestamp": "2025-12-18T14:00:00", "kwh": 2.5, "status": "OK" },
{ "meter_id": "M-101", "timestamp": "2025-12-18T19:30:00", "kwh": 3.0, "status": "OK" },
{ "meter_id": "M-102", "timestamp": "2025-12-18T08:00:00", "kwh": -10.0, "status": "ERR" },
{ "meter_id": "M-103", "timestamp": "2025-12-18T20:00:00", "kwh": 9000.0, "status": "OK" }
]
Analysis:
• Row 1: 2 PM (Standard Rate).
• Row 2: 7:30 PM (Peak Rate - Price should double).
• Row 3: Negative Usage (Bad Data).
• Row 4: 9000 kWh Spike (Bad Data).
Exhibit 2: pricing_plans.csv (Reference Data)
• PlanID,PlanName,BasePricePerKwh
• P1,Saver,0.10
• P2,Eco-Green,0.25
Exhibit 3: customer_pii.csv (Sensitive Data)
• MeterID,PlanID,CustomerEmail
• M-101,P1,sarah.j@example.com
• M-102,P1,mike.t@example.com
• M-103,P2,jenny.w@example.com
The Challenge: The "Smart Logic" Pipeline
You must build a pipeline that ingests the data and a PySpark Notebook that applies the logic.
Phase 1: Ingestion
• Use a Data Pipeline to move the raw files into the Lakehouse Files (Bronze) section.
Phase 2: Transformation Logic (PySpark Notebook)
You must write code to handle the following three requirements simultaneously:
Requirement A: The Quality Filter
• Rule: kwh must be > 0 AND kwh must be < 100.
• Action: Write bad rows to a table named Quarantine_Readings. Keep good rows for processing.
Requirement B: The Security Hash
• Rule: Join the valid readings with customer_pii.csv.

CloudMesh360 | Center of Excellence for Microsoft Fabric

This case study is part of Microsoft Boot Camp #2 led by Surya Kumar (CoE) in Dec 2025 Page 3 of 3
• Action: You must not write the CustomerEmail column to the Silver table. Instead, create a new
column CustomerHash using the SHA-256 algorithm to anonymize the email.
Requirement C: The "Peak Hour" Math
• Rule: Look at the timestamp of the reading.
o If the time is between 18:00 (6 PM) and 21:00 (9 PM): FinalRate = BasePricePerKwh * 2.
o Otherwise: FinalRate = BasePricePerKwh.
• Calculation: Bill_Amount = kwh * FinalRate.
Phase 3: Storage
• Save the clean, hashed, and calculated data to a Delta Table named Silver_SmartBill.
Key Questions for Discussion
1. Complexity: Why is SHA-256 hashing better than just deleting the email column? (Hint: How would
Support help a customer if we deleted their ID entirely?)
2. Logic: In Exhibit 1, Row 2 occurs at 19:30 (7:30 PM). If the base rate is $0.10, what should the final
Bill_Amount be? (Show your math: 3.0 kwh * ($0.10 \* 2)\).
3. Architecture: Why must we perform the "Quality Filter" before the "Peak Hour Math"? What
happens if we try to calculate a bill for -10 kWh?
Lab Deliverables
1. PySpark Notebook: Showing the code for:
o Filtering (Splitting Good vs. Bad).
o Hashing the Email (Security).
o Conditional Logic (when/otherwise) for the Peak Pricing.
2. Two Tables:
o Quarantine_Readings: Must contain the negative reading and the 9000 spike.
o Silver_SmartBill: Must contain the valid readings, the Hashed email (no raw text), and the
correctly calculated bill.

3. Validation: A SQL query selecting the top 5 rows from Silver_SmartBill to prove the emails are
hashed.


