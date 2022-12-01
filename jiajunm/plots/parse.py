import re

def parse_sim_result(path: str):
    result = {}
    
    for row in open(path, 'r'):
        matcher = re.findall(r'\((.*)\+(.*)\)\((.*)\+(.*)\) (.*) (.*) (.*) (.*) (.*) (.*) (.*) (.*) (.*)', row)
        
        (k_net_, p_net_, k_local_, p_local_, total_drives_, afr_, cap_, io_speed_, nines_, sigma_, failed_iters_, total_iters_, adapt_) = matcher[0]
        
        result['k_net'] = (result.get('k_net', []) + [int(k_net_)])
        result['k_local'] = (result.get('k_local', []) + [int(k_local_)])
        result['p_net'] = (result.get('p_net', []) + [int(p_net_)])
        result['p_local'] = (result.get('p_local', []) + [int(p_local_)])
        result['total_drives'] = (result.get('total_drives', []) + [int(total_drives_)])
        result['afr'] = (result.get('afr', []) + [float(afr_)])
        result['cap'] = (result.get('cap', []) + [int(cap_)])
        result['io_speed'] = (result.get('io_speed', []) + [int(io_speed_)])
        result['nines'] = (result.get('nines', []) + [float(nines_)])
        result['sigma'] = (result.get('sigma', []) + [float(sigma_)])
        result['failed_iters'] = (result.get('failed_iters', []) + [int(failed_iters_)])
        result['total_iters'] = (result.get('total_iters', []) + [int(float(total_iters_))])
        
    return result

def parse_calc_result(path: str):
    result = {}
    
    for row in open(path, 'r'):
        matcher = re.findall(r'(.*) (.*)', row)
        (calc_afr_, calc_nines_) = matcher[0]
        
        result['afr'] = (result.get('afr', []) + [calc_afr_])
        result['nines'] = (result.get('nines', []) + [calc_nines_])
    
    return result

# if __name__ == "__main__":
#     print(parse_result('src/s-result-DP_NET.log'))