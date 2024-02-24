#include <stdio.h>
#include<sys/time.h>
#include <string.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include "defines.h"
#include <errno.h>
#include <stdlib.h>
#include <time.h>


int connect_client(char *argv[])
{
	int sock;
	struct sockaddr_in server;

	//Create socket
	sock = socket(AF_INET, SOCK_STREAM, 0);
	if (sock == -1)
	{
		printf("Could not create socket\n");
		return EXIT_FAILURE;
	}

	// printf("Socket created\n");

	server.sin_addr.s_addr = inet_addr(argv[1]);
	server.sin_family = AF_INET;
	server.sin_port = htons(PORT);

	if (connect(sock, (struct sockaddr *) &server, sizeof(server)) < 0)
	{
		perror("connect failed. Error");
		return EXIT_FAILURE;
	}

	return sock;
}

int connect_server()
{
	int socket_desc, client_sock, c;
	struct sockaddr_in server, client;

	//Create socket
	socket_desc = socket(AF_INET, SOCK_STREAM, 0);
	if (socket_desc == -1)
	{
		printf("Could not create socket\n");
		return EXIT_FAILURE;
	}

	// puts("Socket created");

	//Prepare the sockaddr_in structure
	server.sin_family = AF_INET;
	server.sin_addr.s_addr = INADDR_ANY;
	server.sin_port = htons(PORT);

	// Bind
	if (bind(socket_desc, (struct sockaddr *) &server, sizeof(server)) < 0)
	{
		//print the error message
		perror("bind failed. Error");
		return EXIT_FAILURE;
	}

	printf("bind done\n");

	//Listen
	listen(socket_desc, 3);

	//Accept and incoming connection
	printf("Waiting for incoming connections...\n");

	c = sizeof(struct sockaddr_in);

	//accept connection from an incoming client
	client_sock = accept(socket_desc, (struct sockaddr *) &client, (socklen_t *) &c);
	if (client_sock < 0)
	{
		perror("accept failed");
		return EXIT_FAILURE;
	}

	close(socket_desc);

	return client_sock;
}

size_t handle_sock_operation(int sock, char *buffer, int size, int is_send)
{
	if (is_send)
	{
		return send(sock, buffer, size, 0);
	}

	// If got here - it's receiving:
	// Recv returns even if 5 bytes are available, even though expecting 15, for example.
	// So will continue to read until it has all arrived
	size_t bytes_received = 0;
	size_t result = 0;
	while (bytes_received < size)
	{
		result = recv(sock, buffer + bytes_received, size - bytes_received, 0);
		if (result == 0)
		{
			printf("socket was closed remotely\n");
		}
		else if (result < 0)
		{
			// socket was closed on remote end or hit an error
			// either way, the socket is likely dead
			return result;
		}
		else
		{
			bytes_received += result;
		}
	}

	return result;
}

long diff(struct timespec *start, struct timespec *end)
{
	struct timespec temp;
	if ((end->tv_nsec - start->tv_nsec) < 0)
	{
		temp.tv_sec = end->tv_sec - start->tv_sec - 1;
		temp.tv_nsec = 1000000000 + end->tv_nsec - start->tv_nsec;
	}
	else
	{
		temp.tv_sec = end->tv_sec - start->tv_sec;
		temp.tv_nsec = end->tv_nsec - start->tv_nsec;
	}

	return temp.tv_sec * 1000000000 + temp.tv_nsec;
}

int main(int argc, char *argv[])
{
	int sock = 0;
	char buffer[MESSAGE_LENGTH];
	int curr_size_to_send = 1;
	size_t ret_val = 0;
	int is_client = 0;
	struct timespec st, et;
	double measurements[EXP_AMOUNT] = { 0 };
	long nanosec_elapsed = 0;

	if (argc > 1)
	{
		is_client = 1;
	}

	if (is_client)
	{
		sock = connect_client(argv);
	}
	else
	{
		sock = connect_server();
	}

	if (sock == EXIT_FAILURE)
	{
		return EXIT_FAILURE;
	}

	printf("Connected!\n");

	for (int j = 0; j <= EXP_AMOUNT; j++)
	{
		for (int i = 0; i < CLIENT_WARMUP_MSG_AMOUNT; i++)
		{
			ret_val = handle_sock_operation(sock, buffer, curr_size_to_send, is_client);
			if (ret_val < 0)
			{
				printf("Warmup at line %d failed, errno: %s\n", __LINE__, strerror(errno));
				return EXIT_FAILURE;
			}
		}

		nanosec_elapsed = 0;

		for (int i = 0; i < CLIENT_MSG_AMOUNT; i++)
		{
			// Start counting time
			if (is_client)
			{
				clock_gettime(CLOCK_MONOTONIC, &st);
			}

			// Operation itself
			ret_val = handle_sock_operation(sock, buffer, curr_size_to_send, is_client);

			if (is_client)
			{
				clock_gettime(CLOCK_MONOTONIC, &et);

				// Sometimes et.tv_usec - st.tv_usec is negative, probably numerical error, not enough time has passed
				nanosec_elapsed += diff(&st, &et);
			}

			// Making sure operation did not fail
			if (ret_val < 0)
			{
				printf("Operation at line %d failed, errno: %s\n", __LINE__, strerror(errno));
				return EXIT_FAILURE;
			}
		}

		if (is_client)
		{
			// Normalizing total nanosec_elapsed time
			measurements[j] = ((double) nanosec_elapsed / CLIENT_MSG_AMOUNT) * 1e-9; // Measuring avg SECONDS it took
			//printf("%d\t%ld\tnanoseconds\t\n", curr_size_to_send, measurements[j]);

			// (curr_size_to_send)/1024/1024 MB -> in measurements[j] seconds
			// ? MB -> in 1 second
			double megabits_per_sec = ((double) (8*curr_size_to_send) / (1024 * 1024)) / measurements[j];
			printf("%d\t%f\tMbps\t\n", curr_size_to_send, megabits_per_sec);
		}

		ret_val = handle_sock_operation(sock, buffer, curr_size_to_send, !is_client);
		if (ret_val < 0)
		{
			printf("Operation at line %d failed, errno: %s\n", __LINE__, strerror(errno));
			return EXIT_FAILURE;
		}

		// Exponentially growing packet size
		curr_size_to_send *= 2;
	}

	close(sock);

	return EXIT_SUCCESS;
}

