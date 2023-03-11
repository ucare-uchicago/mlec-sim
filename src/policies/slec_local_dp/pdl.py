def slec_local_dp_pdl(slec_local_dp):
    if slec_local_dp.sys_failed:
        return 1   # data loss
    return 0