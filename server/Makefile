.SUFFIXES: .c .o

COMPILE = $(CC) -Wall -O2 -D_FILE_OFFSET_BITS=64 -DOS_LINUX
#COMPILE = $(CC) -Wall -g -D_FILE_OFFSET_BITS=64 -DOS_LINUX -D__DEBUG__
INC_PATH = -I../common -I../../fastdfs/common -I/usr/local/include \
           -I /usr/local/BerkeleyDB.4.7/include
LIB_PATH = -L/usr/local/lib -lpthread -levent -L /usr/local/BerkeleyDB.4.7/lib -ldb
TARGET_PATH = /usr/local/bin

COMMON_LIB =
SHARED_OBJS = ../../fastdfs/common/hash.o ../../fastdfs/common/fdfs_define.o ../../fastdfs/common/chain.o \
              ../../fastdfs/common/shared_func.o ../../fastdfs/common/ini_file_reader.o \
              ../../fastdfs/common/logger.o ../../fastdfs/common/sockopt.o \
              ../../fastdfs/common/fdfs_base64.o \
              ../common/fdht_global.o ../common/fdht_func.o global.o \
              task_queue.o recv_thread.o send_thread.o db_op.o service.o

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
	cp -f $(ALL_PRGS) $(TARGET_PATH)
clean:
	rm -f $(ALL_OBJS) $(ALL_PRGS)
