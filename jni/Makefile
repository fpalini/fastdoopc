.PHONY: all lib bin clean

export

CXX = g++-9
CXXFLAGS = -O2 -m64
CXXFLAGS += -DNDEBUG -D_FILE_OFFSET_BITS=64 -D_LARGEFILE64_SOURCE
CXXFLAGS += -Wall
CXXFLAGS += -std=c++11 -fPIC -I /usr/lib/jvm/java-8-openjdk-amd64/include/ -I /usr/lib/jvm/java-8-openjdk-amd64/include/linux

DEP_LIBS += -lpthread
BIN_DIR = bin

APP_NAME = dsrc
LIB_NAME = libcodec.so
LIB_DIR = .
SRC_DIR = src

bin:
	cd src; ${MAKE} bin
	test -d $(BIN_DIR) || mkdir $(BIN_DIR)
	mv $(SRC_DIR)/$(APP_NAME) $(BIN_DIR)/

lib:
	cd src; ${MAKE} lib
	test -d $(LIB_DIR) || mkdir $(LIB_DIR)
	mv $(SRC_DIR)/$(LIB_NAME) $(LIB_DIR)/

clean:
	-rm $(LIB_NAME)
