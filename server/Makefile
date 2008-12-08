.SUFFIXES: .c .o

COMPILE = $(CC) -O3 -Wall -D_FILE_OFFSET_BITS=64 -DOS_LINUX
INC_PATH = -I../common -I/usr/local/include
LIB_PATH = -L/usr/local/lib -levent -ldb  -lpthread
TARGET_PATH = /usr/local/bin

COMMON_LIB =
SHARED_OBJS = ../common/hash.o  ../common/chain.o \
              ../common/shared_func.o ../common/ini_file_reader.o \
              ../common/logger.o ../common/sockopt.o \
              ../common/fdfs_base64.o ../common/sched_thread.o \
              ../common/fdht_global.o ../common/fdht_func.o \
              ../common/fdht_proto.o \
              global.o task_queue.o recv_thread.o send_thread.o db_op.o \
              func.o work_thread.o sync.o db_recovery.o

ALL_OBJS = $(SHARED_OBJS)

ALL_PRGS = fdhtd

all: $(ALL_OBJS) $(ALL_PRGS)
.o:
	$(COMPILE) -o $@ $<  $(SHARED_OBJS) $(COMMON_LIB) $(LIB_PATH) $(INC_PATH)
.c:
	$(COMPILE) -o $@ $<  $(ALL_OBJS) $(COMMON_LIB) $(LIB_PATH) $(INC_PATH)
.c.o:
	$(COMPILE) -c -o $@ $<  $(INC_PATH)
install:
	cp -f $(ALL_PRGS) /usr/local/bin
clean:
	rm -f $(ALL_OBJS) $(ALL_PRGS)
