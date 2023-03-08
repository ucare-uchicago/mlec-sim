def slec_local_cp_pdl(slec_local_cp):
    if slec_local_cp.sys_failed:
        return 1   # data loss
    return 0