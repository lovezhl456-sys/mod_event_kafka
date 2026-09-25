# Customise these as appropriate
MODNAME = mod_event_kafka.so
MODOBJ = mod_event_kafka.o src/kafka_outbox.o src/kafka_pipeline.o
MODCFLAGS = -Wall -Werror 
MODLDFLAGS = -lssl -lsqlite3 -lrdkafka -lpthread 

CXX = g++
CXXFLAGS = -fPIC -g -ggdb -I/usr/include -Iinclude `pkg-config --cflags freeswitch` $(MODCFLAGS) -std=c++17 -fpermissive -O2
LDFLAGS = `pkg-config --libs freeswitch` -lrdkafka -lz -lpthread -lrt $(MODLDFLAGS) 

.PHONY: all
all: $(MODNAME)

$(MODNAME): $(MODOBJ)
	@$(CXX) -shared -o $@ $(MODOBJ) $(LDFLAGS)

%.o: %.cpp
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
