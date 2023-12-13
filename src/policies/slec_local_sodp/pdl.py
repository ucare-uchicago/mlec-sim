def slec_local_sodp_pdl(slec_local_sodp):
    if slec_local_sodp.sys_failed:
        return 1   # data loss
    return 0