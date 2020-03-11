LD      := gcc-7
LDFLAGS := -O3 -std=gnu11 -flto -lgsl -lgslcblas -libverbs -lrt -lpthread -lmemcached -lnuma -lrdmacm
CC  = gcc-7
CFLAGS   = -O3 -std=gnu11 -g -I../../include/mica -I../../include/libhrd -I../../include/zookeeper -I../../include/optik
APPS    := zookeeper clean-o
PROF    := -g -fno-omit-frame-pointer

all: ${APPS}

zookeeper: ../libhrd/hrd_conn.o ../libhrd/hrd_util.o ../mica/mica.o ../mica/city.o main.o leader.o follower.o cache.o util.o stats.o
	${LD} -o $@ $^ ${LDFLAGS}



PHONY: clean
clean:
	rm -f *.o ../libhrd/*.o ../mica/*.o ${APPS}
clean-o:
	rm -f *.o ../libhrd/*.o ../mica/*.o
