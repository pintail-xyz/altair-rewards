import time
import json
import math
import random

import psycopg2

def print_progress(start_time, current_item, n_items):
    seconds = time.time() - start_time
    elapsed = time.strftime("%H:%M:%S", time.gmtime(seconds))
    left = time.strftime(
        "%H:%M:%S", time.gmtime(seconds * ((n_items) / (current_item + 1) - 1))
    )
    perc = 100 * (current_item + 1) / n_items
    print(
        f"iteration {current_item} of {n_items} ({perc:.2f}%) "
        f"/ {elapsed} elapsed / {left} left", end='\r'
    )

COMMITTEE_EPOCHS = 256
COMMITTEE_VALIDATORS = 512
BR_FACTOR = 64
EFFECTIVE_BALANCE = 32 * int(1e9) # assume all validators have max effective balance
BR_PER_EPOCH = 4 # applies for pre-Altair base reward calc
H_WEIGHT = 14
S_WEIGHT = 14
T_WEIGHT = 26
S_WEIGHT = 2
PROPOSER_WEIGHT = 8
WEIGHT_DENOMINATOR = 64
ALTAIR_PROPOSER_REWARD_FACTOR = 4
RESULTS_FILENAME = 'rewards.json'
CHECK_FILENAME = 'check.json'
NUM_EPOCHS = 32000


start_time = time.time()
connection = psycopg2.connect(
    user="chain", host="127.0.0.1", database="chain", password="medalla"
)
cursor = connection.cursor()

# get inital balances for genesis valildators
cursor.execute(
    "SELECT f_validator_index, f_balance FROM t_validator_balances "
    "WHERE f_epoch = 0"
)
init_balances = {r[0]: r[1] for r in cursor.fetchall()}
net_rewards = {k: 0 for k in init_balances}

for e in range(NUM_EPOCHS):
    cursor.execute(
        f"SELECT f_active_balance FROM t_epoch_summaries WHERE f_epoch = {e}"
    )
    active_balance = cursor.fetchone()[0]
    alt_br = EFFECTIVE_BALANCE * BR_FACTOR // math.isqrt(active_balance)
    total_br = active_balance * BR_FACTOR // math.isqrt(active_balance)
    per_epoch_sync_reward = total_br * S_WEIGHT // WEIGHT_DENOMINATOR // COMMITTEE_VALIDATORS
    old_br = alt_br // BR_PER_EPOCH
    cursor.execute(
        f"SELECT * FROM t_validator_epoch_summaries WHERE f_epoch = {e}"
    )
    # 0 validator_index, 1 epoch, 2 proposer_duties, 3 proposals_included, 
    # 4 attestation_included, 5 target_correct, 6 head_correct, 7 inclusion_delay
    att_performance = cursor.fetchall()
    source_balance = head_balance = target_balance = 0
    
    # altair total attesting balance (head, source and target) in this epoch
    for row in att_performance:
        if row[6] and row[7] <= 1:
            head_balance += EFFECTIVE_BALANCE
        if row[4] and row[7] <= 5:
            source_balance += EFFECTIVE_BALANCE
        if row[5]:
            target_balance += EFFECTIVE_BALANCE
    
    # altair attestation rewards for this epoch
    head_reward = alt_br * head_balance * H_WEIGHT // active_balance // WEIGHT_DENOMINATOR
    source_reward = alt_br * source_balance * S_WEIGHT // active_balance // WEIGHT_DENOMINATOR
    target_reward = alt_br * target_balance * T_WEIGHT // active_balance // WEIGHT_DENOMINATOR

    # randomly select sync committee
    if e % COMMITTEE_EPOCHS == 0:
        committee = [r[0] for r in random.sample(att_performance, COMMITTEE_VALIDATORS)]

    # calculate the net reward for each validator (attestation and sync committee)
    for row in att_performance:
        alt_reward = 0
        if row[6] and row[7] <= 1: # head reward - only given if included in 1 slot
            alt_reward += head_reward
        else:
            alt_reward -= alt_br * H_WEIGHT // WEIGHT_DENOMINATOR
        if row[4] and row[7] <= 5: # source reward - only given if included in 5 slots
            alt_reward += source_reward
        else:
            alt_reward -= alt_br * S_WEIGHT // WEIGHT_DENOMINATOR
        if row[0] in committee:
            if row[4]:
                alt_reward += per_epoch_sync_reward
            else:
                alt_reward -= per_epoch_sync_reward
        if row[5]:
            alt_reward += target_reward # target reward
        else:
            alt_reward -= alt_br * T_WEIGHT // WEIGHT_DENOMINATOR

        if row[0] in net_rewards:
            net_rewards[row[0]] += alt_reward
        else:
            # get initial balance for new validator
            net_rewards[row[0]] = alt_reward
            cursor.execute(
                f"SELECT f_balance FROM t_validator_balances "
                f"WHERE f_validator_index = {row[0]} "
                f"AND f_epoch = {e}"
            )
            init_balances[row[0]] = cursor.fetchone()[0]


    print_progress(start_time, e, NUM_EPOCHS)

# add on all proposer rewards (scaled for altair)
cursor.execute(
    f"SELECT f_validator_index, f_block_reward FROM t_validator_epoch_extras "
    f"WHERE f_epoch = {NUM_EPOCHS - 1}"
)
proposer_rewards = cursor.fetchall()
for row in proposer_rewards:
    net_rewards[row[0]] += row[1] * ALTAIR_PROPOSER_REWARD_FACTOR

# save calcuated net rewards
with open(RESULTS_FILENAME, 'w') as f:
    json.dump(net_rewards, f)

# get the actual final balance for each validator
cursor.execute(
    f"SELECT f_validator_index, f_balance FROM t_validator_balances "
    f"WHERE f_epoch = {NUM_EPOCHS + 1}"
)
check = {i: bal for (i, bal) in cursor.fetchall() if i in net_rewards}
# subtract initial balance to get actual net reward for each vaidator
for i in check:
    check[i] -= init_balances[i]

# save the actual net rewards
with open(CHECK_FILENAME, 'w') as f:
    json.dump(check, f)

print('\ndone')
connection.commit()
cursor.close()
connection.close()
