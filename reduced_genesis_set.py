import json

import psycopg2

FUTURE_EPOCH = 2**64 - 1 # from spec
END_EPOCH = 32000
OUTPUT_FILENAME = 'reduced_genesis_set.json'

connection = psycopg2.connect(
    user="chain", host="127.0.0.1", database="chain", password="medalla"
)
cursor = connection.cursor()

cursor.execute(
    "SELECT f_index, f_activation_epoch, f_exit_epoch, f_slashed, f_public_key "
    "FROM t_validators ORDER BY f_index"
)

validators = [{
    'index': r[0],
    'activation_epoch': FUTURE_EPOCH if r[1] is None else r[1],
    'exit_epoch': FUTURE_EPOCH if r[2] is None else r[2],
    'slashed': r[3],
    'slasher': False,
    'redeposit': False,
    'pubkey': r[4].hex()
} for r in cursor.fetchall()]

pubkey_lookup = {v['pubkey']: v for v in validators}

# identify slashers

cursor.execute("SELECT f_inclusion_slot FROM t_proposer_slashings")
proposer_slashings = [s[0] for s in cursor.fetchall()]

cursor.execute("SELECT f_inclusion_slot FROM t_attester_slashings")
slashings = proposer_slashings + [s[0] for s in cursor.fetchall()]

for s in slashings:
    cursor.execute(f"SELECT f_validator_index FROM t_proposer_duties WHERE f_slot = {s}")
    validators[cursor.fetchone()[0]]["slasher"] = True

# check for any instances of validator deposits made to already active validators

cursor.execute("SELECT f_inclusion_slot, f_validator_pubkey FROM t_deposits")
for row in cursor.fetchall():
    deposit_slot = row[0]
    pubkey = row[1].hex()
    if pubkey not in pubkey_lookup:
        continue
    else:
        validator = pubkey_lookup[pubkey]

    if deposit_slot // 32 > validator["activation_epoch"] and deposit_slot // 32 < validator["exit_epoch"]:
        validator["redeposit"] = True

reduced_genesis_set = [
    v["index"] for v in validators if v["activation_epoch"] == 0 and not (
        v["slashed"] or v["slasher"] or v["redeposit"] or v["exit_epoch"] < END_EPOCH
    )
]

with open(OUTPUT_FILENAME, 'w') as f:
    json.dump(reduced_genesis_set, f)
