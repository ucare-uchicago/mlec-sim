import re

def parse_sim_result(path: str):
    result = {}
    
    for row in open(path, 'r'):
        matcher = re.findall(r'^\(kn:(.*)\+pn:(.*)\)\(kl:(.*)\+pl:(.*)\) td:(.*) afr:(.*) cap:(.*) io:(.*) ibw:(.*) cbw:(.*) nn:(.*) sd:(.*) f:(.*) t:(.*) ad:(.*)', row)
        
        (k_net_, p_net_, k_local_, p_local_, total_drives_, afr_, cap_, io_speed_, intra_rack_, inter_rack_, nines_, sigma_, failed_iters_, total_iters_, adapt_) = matcher[0]
        
        result['k_net'] = (result.get('k_net', []) + [int(k_net_)])
        result['k_local'] = (result.get('k_local', []) + [int(k_local_)])
        result['p_net'] = (result.get('p_net', []) + [int(p_net_)])
        result['p_local'] = (result.get('p_local', []) + [int(p_local_)])
        result['total_drives'] = (result.get('total_drives', []) + [int(total_drives_)])
        result['afr'] = (result.get('afr', []) + [float(afr_)])
        result['cap'] = (result.get('cap', []) + [int(cap_)])
        result['io_speed'] = (result.get('io_speed', []) + [int(io_speed_)])
        result['intra_bw'] = (result.get('intra_bw', []) + [float(intra_rack_)])
        result['inter_bw'] = (result.get('inter_bw', []) + [float(inter_rack_)])
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
        
        result['afr'] = (result.get('afr', []) + [float(calc_afr_)])
        result['nines'] = (result.get('nines', []) + [float(calc_nines_)])
    
    return result

def parse_metric_result(path: str):
    result = {}
    
    for row in open(path, 'r'):
        matcher = re.findall(r'^avg_rebuild_io_per_year:(.*) avg_net_traffic:(.*) avg_failure_count:(.*) avg_rebuild_time:(.*) avg_net_repair_time:(.*) iter_count:(.*) total_net_repair_count:(.*) total_delayed_disks:(.*)', row)
        (avg_rebuild_io_per_year, avg_net_traffic, avg_failure_count, avg_rebuild_time, avg_net_repair_time, iter_count, total_net_repair_count, total_delayed_disks) = matcher[0]
                
        result['avg_rebuild_io_per_year'] = (result.get('avg_rebuild_io_per_year', []) + [float(avg_rebuild_io_per_year)])
        result['avg_net_traffic'] = (result.get('avg_net_traffic', []) + [float(avg_net_traffic)])
        result['avg_failure_count'] = (result.get('avg_failure_count', []) + [float(avg_failure_count)])
        result['avg_rebuild_time'] = (result.get('avg_rebuild_time', []) + [float(avg_rebuild_time)])
        result['avg_net_repair_time'] = (result.get('avg_net_repair_time', []) + [float(avg_net_repair_time)])
        result['iter_count'] = (result.get('iter_count', []) + [float(iter_count)])
        result['total_net_repair_count'] = (result.get('total_net_repair_count', []) + [float(total_net_repair_count)])
        result['total_delayed_disks'] = (result.get('total_delayed_disks', []) + [float(total_delayed_disks)])
    
    return result