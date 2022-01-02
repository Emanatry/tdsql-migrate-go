#include "common.cpp"

bool dual_column_pk = false;
bool triple_column_pk = false;

// compare PK of two records, negative if a<b, 0 if a=b, positive if a>b
int comparePK(const dat &a, const dat &b) {
	if(a.key == b.key) {
		if(dual_column_pk) {
			return strcmp(COL(a.data, 1), COL(b.data, 1)); // compare column `a`
		} else if (triple_column_pk) {
			int cmpres = strcmp(COL(a.data, 1), COL(b.data, 1)); // compare `a`
			if(cmpres != 0) return cmpres;
			return strcmp(COL(a.data, 2), COL(b.data, 2)); // compare `b`
		} else {
			return 0;
		}
	}
	return a.key - b.key;
}

int dupcount = 0;

static dat lastdat;

void flushlastdat(FILE *f) {
	if(lastdat.data == NULL) return;
	writeline(f, lastdat.data);
	delete lastdat.data;
	lastdat.data = NULL;
}

void commit(FILE *f, const dat &buf) {
	if(comparePK(lastdat, buf) == 0) {
		// duplicate key, try merge
		// printf("dup: \n%s,%s,%s,%s\n%s,%s,%s,%s\n", COL(buf.data, 0), COL(buf.data, 1), COL(buf.data, 2), COL(buf.data, 3), COL(lastdat.data, 0), COL(lastdat.data, 1), COL(lastdat.data, 2), COL(lastdat.data, 3));
		dupcount++;
		if(strcmp(COL(lastdat.data, 3), COL(buf.data, 3)) < 0) { // compare `updated_at`, if the record is more recent than lastdat
			// printf("choose prior\n");
			lastdat = buf;
		} else {
			// printf("choose latter\n");
		}
	} else {
		flushlastdat(f);
		lastdat = buf;
	}
}

int main(int argc, char** argv) {
	if(argc < 5) {
		printf("usage: ./%s input1.csv input2.csv output.csv <id|id_a|id_a_b>\n", argv[0]);
		return 1;
	}
	char *inputFile1 = argv[1];
	char *inputFile2 = argv[2];
	char *outputFile = argv[3];
	dual_column_pk = strcmp(argv[4], "id_a") == 0;
	triple_column_pk = strcmp(argv[4], "id_a_b") == 0;

	FILE *r1 = fopen(inputFile1, "r");
	FILE *r2 = fopen(inputFile2, "r");
	FILE *f = fopen(outputFile, "w");
	if(r1 == NULL || r2 == NULL || f == NULL) return -1;

	struct timeval t1, t2;
    gettimeofday(&t1, NULL);

	dat b1 = nextline(r1);
	dat b2 = nextline(r2);
	FILE *remainder;
	while(true) {
		if(b1.data == NULL) {
			commit(f, b2);
			remainder = r2;
			break;
		}
		if(b2.data == NULL) {
			commit(f, b1);
			remainder = r1;
			break;
		}
		if(comparePK(b1, b2) < 0) {
			commit(f, b1);
			b1 = nextline(r1);
		} else {
			commit(f, b2);
			b2 = nextline(r2);
		}
	}

	dat b = nextline(remainder);
	while(b.data != NULL) {
		commit(f, b);
		b = nextline(remainder);
	}

	flushlastdat(f);

	fflush(f);
	fclose(f);
	fclose(r1);
	fclose(r2);
	gettimeofday(&t2, NULL);
	printf("out: %s\ndup: %d\nmerge: %dms\n", outputFile, dupcount, diffms(t2, t1));
}