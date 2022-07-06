from inspect import trace
from poisson import Poisson
import argparse
import csv

class Generator:
    def __init__(self, failRatio, N, traceDir, pipeFile, outputFile):
        self.traceDir = traceDir
        self.failRatio = failRatio;
        self.generate_failures(N, failRatio)

        # if (pipeFile == True):
        #     print("print to file")
        self.print_to_file()
        # else:
        #     print("print to screen")
        #     self.print_to_screen()


    def generate_failures(self, N, failRatio):
        poisson = Poisson(N, failRatio, 86400)
        self.trace_entry = poisson.generate_poisson_failures(print_=False)


    def print_to_file(self, filename="fail_stream_seq_1.csv"):
        with open(filename, 'w') as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(['diskId', 'failTime'])
            for trace in self.trace_entry:
                csvwriter.writerow([trace[1], trace[0]]);
            csvfile.close()
            
def setup_parameters():
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", dest='file', action='store_true')
    parser.add_argument('--screen', dest='file', action='store_false')
    parser.add_argument('--outputFile', type=str, help="output file",default="164models.txt")
    args = parser.parse_args()

    (file, outputFile) = (args.file, args.outputFile)
    return (file, outputFile)

if __name__ == "__main__":
    (file, outputFile) = setup_parameters()
    gen = Generator(1, "", file, outputFile)

