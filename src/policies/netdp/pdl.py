import logging

def network_decluster_pdl(state):
    if state.policy.max_priority > state.sys.m:
            # data loss
            return 1
    return 0