import pandas as pd
import numpy as np
from datetime import datetime, timedelta



np.random.seed(42)

n = 50
start_date = datetime(2025, 3, 1)

transactions = []


for i in range(n):
    date = start_date + timedelta(days=int(np.random.randint(0, 28)))  
    amount = round(float(np.random.uniform(100, 5000)), 2)

    transactions.append({
        "transaction_id": f"T{i+1}",
        "date": date,
        "amount": amount,
        "type": "payment"
    })

transactions_df = pd.DataFrame(transactions)




settlements = []

for _, row in transactions_df.iterrows():
    settle_date = row["date"] + timedelta(days=int(np.random.randint(1, 3))) 

    settlements.append({
        "transaction_id": row["transaction_id"],
        "settle_date": settle_date,
        "amount": row["amount"]
    })

settlements_df = pd.DataFrame(settlements)






settlements_df.loc[0, "settle_date"] = datetime(2025, 4, 2)


settlements_df.loc[1, "amount"] = float(settlements_df.loc[1, "amount"]) + 0.01


duplicate = settlements_df.iloc[[2]].copy()
settlements_df = pd.concat([settlements_df, duplicate], ignore_index=True)


refund = pd.DataFrame([{
    "transaction_id": "R999",
    "settle_date": datetime(2025, 3, 15),
    "amount": -500.0
}])
settlements_df = pd.concat([settlements_df, refund], ignore_index=True)



settlements_df["settle_date"] = pd.to_datetime(settlements_df["settle_date"])
transactions_df["date"] = pd.to_datetime(transactions_df["date"])




duplicates = settlements_df[
    settlements_df.duplicated(subset=["transaction_id"], keep=False)
].copy()

settlements_deduped = settlements_df.drop_duplicates(subset=["transaction_id"], keep="first").copy()

merged = transactions_df.merge(
    settlements_deduped,
    on="transaction_id",
    how="outer",
    indicator=True,
    suffixes=("_txn", "_bank")
)



issues = []

for _, row in merged.iterrows():

    
    if row["_merge"] == "left_only":
        issues.append({
            "transaction_id": row["transaction_id"],
            "issue": "Missing in bank settlement",
            "txn_amount": row["amount_txn"],
            "bank_amount": None,
            "txn_date": row["date"],
            "settle_date": None
        })

    
    elif row["_merge"] == "right_only":
        issues.append({
            "transaction_id": row["transaction_id"],
            "issue": "No matching transaction (refund or error)",
            "txn_amount": None,
            "bank_amount": row["amount_bank"],
            "txn_date": None,
            "settle_date": row["settle_date"]
        })

    else:
        txn_amt = float(row["amount_txn"])
        bank_amt = float(row["amount_bank"])

        
        if abs(txn_amt - bank_amt) > 0.001:
            issues.append({
                "transaction_id": row["transaction_id"],
                "issue": "Amount mismatch (rounding or error)",
                "txn_amount": txn_amt,
                "bank_amount": bank_amt,
                "txn_date": row["date"],
                "settle_date": row["settle_date"]
            })

        
        if pd.notna(row["settle_date"]) and pd.notna(row["date"]):
            if row["settle_date"].month != row["date"].month:
                issues.append({
                    "transaction_id": row["transaction_id"],
                    "issue": "Settled next month",
                    "txn_amount": txn_amt,
                    "bank_amount": bank_amt,
                    "txn_date": row["date"],
                    "settle_date": row["settle_date"]
                })


issues_df = pd.DataFrame(issues)


print("\n=== Issues Found ===")
if issues_df.empty:
    print("No issues found.")
else:
    print(issues_df.to_string(index=False))

print("\n=== Duplicate Entries in Bank Settlements ===")
if duplicates.empty:
    print("No duplicates found.")
else:
    print(duplicates.to_string(index=False))

print("\n=== Summary ===")
print(f"Total transactions:       {len(transactions_df)}")
print(f"Total settlement records: {len(settlements_df)}")
print(f"  — of which duplicates:  {len(duplicates)}")
print(f"Issues found:             {len(issues_df)}")
issue_counts = issues_df["issue"].value_counts() if not issues_df.empty else pd.Series(dtype=int)
for issue_type, count in issue_counts.items():
    print(f"  — {issue_type}: {count}")