#include <stdio.h>
#include <string.h>
#include <sys/time.h>

#define COLSIZE 33
#define COL(buf, idx) ((buf)+(idx)*COLSIZE)

struct dat {
	int key;
	char *data;
};

int diffms(const timeval &end, const timeval &start) {
	return (1000000 * ( end.tv_sec - start.tv_sec ) + end.tv_usec - start.tv_usec) / 1000;
}

dat nextline(FILE *r) {
	char *buf = new char[4*COLSIZE];
	int nfield = 0, ifield = 0;
	int ch;
	while((ch = fgetc(r)) != EOF) {
		if(ch == ',') {
			COL(buf, nfield)[ifield++] = '\0';
			nfield++;
			ifield=0;
			continue;
		}
		if(ch == '\n' || ch == EOF) break;
		COL(buf, nfield)[ifield++] = ch;
	}
	if(ch == EOF) {
		delete buf;
		return dat{0, NULL};
	}
	int key;
	sscanf(COL(buf, 0), "%d", &key);
	return dat{key, buf};
}

inline void writeline(FILE *f, char *buf) {
	fwrite(COL(buf, 0), 1, strlen(COL(buf, 0)), f);
	fputc(',', f);
	fwrite(COL(buf, 1), 1, strlen(COL(buf, 1)), f);
	fputc(',', f);
	fwrite(COL(buf, 2), 1, strlen(COL(buf, 2)), f);
	fputc(',', f);
	fwrite(COL(buf, 3), 1, strlen(COL(buf, 3)), f);
	fputc('\n',f);
}