#include <algorithm>

#include "common.cpp"

bool dual_column_pk = false;
bool triple_column_pk = false;

#define MAXROWS 3340000 // slightly bigger than the actual data set

dat dats[MAXROWS];
int ndat;

bool comp(const dat &a, const dat &b) {
	if(a.key == b.key) {
		if(dual_column_pk) {
			return strcmp(COL(a.data, 1), COL(b.data, 1)) < 0; // compare column `a`
		} else if (triple_column_pk) {
			int cmpres = strcmp(COL(a.data, 1), COL(b.data, 1)); // compare `a`
			if(cmpres != 0) return cmpres < 0;
			return strcmp(COL(a.data, 2), COL(b.data, 2)) < 0; // compare `b`
		}
	}
	return a.key < b.key;
}

int main(int argc, char** argv) {
	if(argc < 4) {
		printf("usage: ./%s input.csv output.csv <id|id_a|id_a_b>\n", argv[0]);
		return 1;
	}
	char *inputFile = argv[1];
	char *outputFile = argv[2];
	dual_column_pk = strcmp(argv[3], "id_a") == 0;
	triple_column_pk = strcmp(argv[3], "id_a_b") == 0;

	FILE *r = fopen(inputFile, "r");
	FILE *f = fopen(outputFile, "w");
	if(r == NULL || f == NULL) return -1;

	struct timeval t1, t2, t3, t4;

    gettimeofday(&t1, NULL); // read
	while(true) {
		dat d = nextline(r);
		if(d.data == NULL) break;
		dats[ndat++] = d;
	}
	gettimeofday(&t2, NULL); // sort
	std::sort(dats, dats+ndat, comp);
	gettimeofday(&t3, NULL); // write back
	for(int i=0;i<ndat;i++) {
		char *buf = dats[i].data;
		writeline(f, buf);
		// fprintf(f, "%s,%s,%s,%s\n", COL(buf, 0), COL(buf, 1), COL(buf, 2), COL(buf, 3));
	}
	fflush(f);
	fclose(f);
	fclose(r);
	gettimeofday(&t4, NULL);
	printf("out: %s\nread: %dms\nsort: %dms\nwrite: %dms\n", outputFile, diffms(t2, t1), diffms(t3, t2), diffms(t4, t3));
}