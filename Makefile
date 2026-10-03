# 按需调整以下项
MODNAME = mod_event_kafka.so
MODOBJ = mod_event_kafka.o src/kafka_outbox.o src/kafka_pipeline.o
MODCFLAGS = -Wall -Werror 
MODLDFLAGS = -lssl -lsqlite3 -lrdkafka -lpthread 

CXX = g++
CXXFLAGS = -fPIC -g -ggdb -I/usr/include -Iinclude `pkg-config --cflags freeswitch rdkafka` $(MODCFLAGS) -std=c++17 -fpermissive -O2
LDFLAGS = `pkg-config --libs freeswitch` -lrdkafka -lz -lpthread -lrt $(MODLDFLAGS) 

.PHONY: all
all: $(MODNAME)

$(MODNAME): $(MODOBJ) | check-deps
	@$(CXX) -shared -o $@ $(MODOBJ) $(LDFLAGS)

%.o: %.cpp | check-deps
	@$(CXX) $(CXXFLAGS) -c -o $@ $<

.PHONY: clean
clean:
	rm -f $(MODNAME) $(MODOBJ)

.PHONY: install
install: $(MODNAME)
	install -d $(DESTDIR)/usr/lib/freeswitch/mod
	install $(MODNAME) $(DESTDIR)/usr/lib/freeswitch/mod
	install -d $(DESTDIR)/etc/freeswitch/autoload_configs
	install event_kafka.conf.xml $(DESTDIR)/etc/freeswitch/autoload_configs/

.PHONY: release
release: $(MODNAME)
	distribution/make-deb.sh

.PHONY: check-deps
check-deps:
	@pkg-config --exists freeswitch || { echo "A real FreeSWITCH SDK is required (see docs/CI-BUILD.md)." >&2; exit 1; }
	@pkg-config --atleast-version=2.0.2 rdkafka || { echo "librdkafka >= 2.0.2 is required; Debian 9/old librdkafka is unsupported." >&2; exit 1; }
