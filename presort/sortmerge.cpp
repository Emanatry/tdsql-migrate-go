#include <algorithm>

#include "common.cpp"

bool dual_column_pk = false;
bool triple_column_pk = false;
bool id_b_a_pk = false;

#define MAXROWS 2*3340000 // slightly bigger than the actual data set

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
		} else if (id_b_a_pk) {
			int cmpres = strcmp(COL(a.data, 2), COL(b.data, 2)); // compare `b`
			if(cmpres != 0) return cmpres < 0;
			return strcmp(COL(a.data, 1), COL(b.data, 1)) < 0; // compare `a`
		}
	}
	return a.key < b.key;
}
int comparePK(const dat &a, const dat &b) {
	if(a.key == b.key){
		if(dual_column_pk) {
			return strcmp(COL(a.data, 1), COL(b.data, 1)); // compare column `a`
		} else if (triple_column_pk) {
			int cmpres = strcmp(COL(a.data, 1), COL(b.data, 1)); // compare `a`
			if(cmpres != 0) return cmpres;
			return strcmp(COL(a.data, 2), COL(b.data, 2)); // compare `b`
		} else if (id_b_a_pk) {
			int cmpres = strcmp(COL(a.data, 2), COL(b.data, 2)); // compare `b`
			if(cmpres != 0) return cmpres;
			return strcmp(COL(a.data, 1), COL(b.data, 1)); // compare `a`
		} else {
			return 0;
		}
	}
	return a.key - b.key;
}
int main(int argc, char** argv) {
	if(argc < 5) {
		printf("usage: ./%s input1.csv input2.csv output.csv <id|id_a|id_a_b|id_b_a>\n", argv[0]);
		return 1;
	}
	char *inputFile1 = argv[1];
	char *inputFile2 = argv[2];
	char *outputFile = argv[3];
	dual_column_pk = strcmp(argv[4], "id_a") == 0;
	triple_column_pk = strcmp(argv[4], "id_a_b") == 0;
	id_b_a_pk = strcmp(argv[4], "id_b_a") == 0;

	FILE *r1 = fopen(inputFile1, "r");
	FILE *r2 = fopen(inputFile2, "r");
	FILE *f = fopen(outputFile, "w");
	if(r1 == NULL || r2 == NULL || f == NULL) return -1;

	struct timeval t1, t2, t3, t4;

    gettimeofday(&t1, NULL); // read
	while(true) {
		dat d = nextline(r1);
		if(d.data == NULL) break;
		dats[ndat++] = d;
	}
    while(true) {
		dat d = nextline(r2);
		if(d.data == NULL) break;
		dats[ndat++] = d;
	}
	gettimeofday(&t2, NULL); // sort
	std::sort(dats, dats+ndat, comp);
	gettimeofday(&t3, NULL); // write back
    writeline(f, dats[0].data);
	for(int i=0;i<ndat;i++) {
		char *buf = dats[i].data;
        //writeline(f, buf);
		if(i>0&&comparePK(dats[i],dats[i-1])!=0)writeline(f, buf);
		//fprintf(f, "%s,%s,%s,%s\n", COL(buf, 0), COL(buf, 1), COL(buf, 2), COL(buf, 3));
	}
	fflush(f);
	fclose(f);
	fclose(r1);
	fclose(r2);
	gettimeofday(&t4, NULL);
	printf("out: %s\nread: %dms\nsort: %dms\nwrite: %dms\n", outputFile, diffms(t2, t1), diffms(t3, t2), diffms(t4, t3));
}
