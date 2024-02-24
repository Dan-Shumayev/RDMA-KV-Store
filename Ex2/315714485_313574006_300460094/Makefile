all: server client

server:
	gcc bw_template.c -o server -libverbs

client:
	gcc bw_template.c -o client -libverbs

clean:
	rm server client
