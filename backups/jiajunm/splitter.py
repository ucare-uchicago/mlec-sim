import sys

fileName = sys.argv[1]
inf = "99999999999.0"
bws = [8,7,6,5]

for bw_ in bws:
    bw = str(bw_).replace('.', '_')
    
    file = open(fileName, "r")
    rows = []
    for row in file:
        if bw_ == "inf":
            bwName = "kl:" + str(inf)
        else:
            bwName = "kl:" + str(bw_)
        if bwName in row:
            rows.append(row)
    
    dirList = fileName.split("/")
    dirList.pop()
    dirPath = '/'.join(dirList)
    
    
    if bw_ == inf:
        dirPath += "/s-result-DP_NET_inf.log"
    else:
        dirPath += "/s-result-DP_NET_" + bw + ".log"
    
    writeFile = open(dirPath, "w")
    for row in rows:
        writeFile.write(row)
    writeFile.flush()
    writeFile.close()