import logging

def slec_net_dp_pdl(state):
    if state.policy.max_priority > state.sys.top_m:
            # data loss
            return 1
    return 0