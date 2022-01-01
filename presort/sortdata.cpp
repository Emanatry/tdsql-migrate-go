#include <algorithm>
#include <stdio.h>
#include <string.h>
#include <sys/time.h>

using namespace std;

#define COLSIZE 33
#define COL(buf, idx) ((buf)+(idx)*COLSIZE)

struct dat {
	int key;
	char *data;
};

#define MAXROWS 3330000 // slightly bigger than the actual data set

dat dats[MAXROWS];
int ndat;

bool comp(const dat &a, const dat &b) {
	if(a.key == b.key) return strcmp(COL(a.data, 2), COL(b.data, 2)); // compare column `b`
	return a.key < b.key;
}

int diffms(const timeval &end, const timeval &start) {
	return (1000000 * ( end.tv_sec - start.tv_sec ) + end.tv_usec - start.tv_usec) / 1000;
}

int main(int argc, char** argv) {
	if(argc < 3) {
		printf("usage: ./%s input.csv output.csv\n", argv[0]);
		return 1;
	}
	char *inputFile = argv[1];
	char *outputFile = argv[2];

	FILE *r = fopen(inputFile, "r");
	FILE *f = fopen(outputFile, "w");
	// setvbuf(f, nullptr, _IOFBF, 20480);
	struct timeval t1, t2, t3, t4;
    gettimeofday(&t1, NULL);
	while(true) {
		char *buf = new char[4*COLSIZE];
		int nfield = 0, ifield = 0;
		int ch;
		while((ch = fgetc(r)) != EOF) {
			if(ch == ',') { nfield++; ifield=0; continue; }
			if(ch == '\n' || ch == EOF) break;
			COL(buf, nfield)[ifield++] = ch;
		}
		if(ch == EOF) {
			delete buf;
			break;
		}
		sscanf(COL(buf, 0), "%d", &dats[ndat].key);
		dats[ndat].data = buf;
		ndat++;
	}
	gettimeofday(&t2, NULL);
	std::sort(dats, dats+ndat, comp);
	gettimeofday(&t3, NULL);
	for(int i=0;i<ndat;i++) {
		char *buf = dats[i].data;
		fwrite(COL(buf, 0), 1, strlen(COL(buf, 0)), f);
		fputc(',', f);
		fwrite(COL(buf, 1), 1, strlen(COL(buf, 1)), f);
		fputc(',', f);
		fwrite(COL(buf, 2), 1, strlen(COL(buf, 2)), f);
		fputc(',', f);
		fwrite(COL(buf, 3), 1, strlen(COL(buf, 3)), f);
		fputc('\n',f);
		// fprintf(f, "%s,%s,%s,%s\n", COL(buf, 0), COL(buf, 1), COL(buf, 2), COL(buf, 3));
	}
	fflush(f);
	fclose(f);
	gettimeofday(&t4, NULL);
	printf("out: %s\nread: %dms\nsort: %dms\nwrite: %dms\n", outputFile, diffms(t2, t1), diffms(t3, t2), diffms(t4, t3));
}