.SUFFIXES: .c .o

COMPILE = $(CC) -Wall -O2 -D_FILE_OFFSET_BITS=64 -DOS_LINUX
#COMPILE = $(CC) -Wall -g -D_FILE_OFFSET_BITS=64 -DOS_LINUX -D__DEBUG__
INC_PATH = -I../fastdfs/common -I/usr/local/include
LIB_PATH = -L/usr/local/lib -lpthread -levent
TARGET_PATH = /usr/local/bin

COMMON_LIB =
SHARED_OBJS = ../fastdfs/common/hash.o ../fastdfs/common/fdfs_define.o ../fastdfs/common/chain.o \
              ../fastdfs/common/shared_func.o ../fastdfs/common/ini_file_reader.o \
              ../fastdfs/common/logger.o ../fastdfs/common/sockopt.o ../fastdfs/common/fdfs_global.o \
              ../fastdfs/common/fdfs_base64.o \
              fdht_global.o task_queue.o recv_thread.o send_thread.o

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
