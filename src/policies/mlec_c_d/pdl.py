def mlec_c_d_pdl(state):
    prob = 0
    for i in range(state.policy.num_rack_groups):
        if state.policy.rack_group_failures[i] > state.sys.top_m:
            prob = 1
            return prob
    return prob