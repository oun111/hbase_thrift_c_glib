
GEN_DIR:= $(shell pwd)/.gen/
C_GLIB_DIR:=./gen-c_glib

C_SRCS         := $(notdir $(shell find . -maxdepth 1 -name "*.c"))
C_GLIB_SRCS    := $(notdir $(shell find $(C_GLIB_DIR) -maxdepth 1 -name "*.c"))
C_OBJS         := $(patsubst %.c,$(GEN_DIR)/%.o,$(C_SRCS))
C_GLIB_OBJS    := $(patsubst %.c,$(GEN_DIR)/%.o,$(C_GLIB_SRCS))
OBJS           := $(C_OBJS) $(C_GLIB_OBJS)


THRIFT_BASE    := /home/user1/work/thrift-0.12.0/install

LDFLAG         := -L$(THRIFT_BASE)/lib -lthrift -lthrift_c_glib -lglib-2.0 -lgobject-2.0 
CFLAG_OBJS     := -Wall -Werror -g -I. -I$(C_GLIB_DIR) -I$(THRIFT_BASE)/include -I/usr/include/glib-2.0/
TARGET_EXEC    := th_app
 

.PHONY: all
all: mkdir  $(TARGET_EXEC) 

mkdir:
	$(shell mkdir  $(GEN_DIR))

$(TARGET_EXEC): $(OBJS)
	$(CC) $(OBJS) -o $@  $(LDFLAG)

%.o:../$(C_GLIB_DIR)/%.c
	$(CC) $(CFLAG_OBJS) -c $< -o $@

%.o:../%.c
	$(CC) $(CFLAG_OBJS) -c $< -o $@

.PHONY: clean
clean: 
	rm -rf $(OBJS) $(TARGET_EXEC) $(GEN_DIR)

.PHONY: distclean
distclean:clean
	rm -rf cscope.* 
	rm -rf tags

