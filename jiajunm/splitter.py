import sys

fileName = sys.argv[1]
inf = "99999999999.0"
bws = [0.1, 0.5, 1, 2, 4, inf]

for bw_ in bws:
    bw = str(bw_).replace('.', '_')
    
    file = open(fileName, "r")
    rows = []
    for row in file:
        if bw_ == "inf":
            bwName = "ibw:" + str(inf)
        else:
            bwName = "ibw:" + str(bw_)
        if bwName in row:
            rows.append(row)
    
    dirList = fileName.split("/")
    dirList.pop()
    dirPath = '/'.join(dirList)
    
    
    if bw_ == inf:
        dirPath += "/s-result-MLEC_inf.log"
    else:
        dirPath += "/s-result-MLEC_" + bw + ".log"
    
    writeFile = open(dirPath, "w")
    for row in rows:
        writeFile.write(row)
    writeFile.flush()
    writeFile.close()