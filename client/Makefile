.SUFFIXES: .c .o .lo

COMPILE = $(CC) -g -Wall -D_FILE_OFFSET_BITS=64 -DOS_LINUX -D__DEBUG__
INC_PATH = -I../common -I../../fastdfs/common -I../tracker -I/usr/local/include
LIB_PATH = -L/usr/local/lib  -lpthread
TARGET_PATH = /usr/local/bin

STATIC_OBJS = ../../fastdfs/common/hash.o ../../fastdfs/common/chain.o \
              ../../fastdfs/common/shared_func.o ../../fastdfs/common/ini_file_reader.o \
              ../../fastdfs/common/logger.o ../../fastdfs/common/sockopt.o \
              ../../fastdfs/common/fdfs_base64.o \
              ../common/fdht_global.o ../common/fdht_proto.o fdht_client.o

SHARED_OBJS = ../../fastdfs/common/hash.lo ../../fastdfs/common/chain.lo \
              ../../fastdfs/common/shared_func.lo ../../fastdfs/common/ini_file_reader.lo \
              ../../fastdfs/common/logger.lo ../../fastdfs/common/sockopt.lo \
              ../../fastdfs/common/fdfs_base64.lo \
              ../common/fdht_global.lo ../common/fdht_proto.lo fdht_client.lo

HEADER_FILES = ../../fastdfs/common/common_define.h ../../fastdfs/common/fdfs_define.h \
               ../../fastdfs/common/fdfs_global.h \
               ../../fastdfs/common/fdfs_base64.h ../../fastdfs/common/shared_func.h \
               ../../fastdfs/common/ini_file_reader.h ../../fastdfs/common/_os_bits.h

ALL_OBJS = $(STATIC_OBJS) $(SHARED_OBJS)

ALL_PRGS = fdht_test
ALL_LIBS = 
#libfdfsclient.so.1

all: $(ALL_OBJS) $(ALL_PRGS) $(ALL_LIBS)
libfdfsclient.so.1:
	$(COMPILE) -o $@ $< -shared -Wl,-soname,libfdfsclient.so.1 $(SHARED_OBJS) $(LIB_PATH)
.o:
	$(COMPILE) -o $@ $<  $(STATIC_OBJS) $(LIB_PATH) $(INC_PATH)
.c:
	$(COMPILE) -o $@ $<  $(STATIC_OBJS) $(LIB_PATH) $(INC_PATH)
.c.o:
	$(COMPILE) -c -o $@ $<  $(INC_PATH)
.c.lo:
	$(COMPILE) -c -fPIC -o $@ $<  $(INC_PATH)
install:
	cp -f $(ALL_PRGS) /usr/local/bin
	cp -f $(ALL_LIBS) /usr/local/lib/
	cp -f $(HEADER_FILES) /usr/local/include/
#	ln -s /usr/local/lib/libfdfsclient.so.1 /usr/local/lib/libfdfsclient.so
clean:
	rm -f $(ALL_OBJS) $(ALL_PRGS) $(ALL_LIBS)
