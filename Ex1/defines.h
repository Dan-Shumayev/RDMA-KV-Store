//
// Created by xGalS on 12/04/2022.
//

#ifndef EX1_DEFINES_H
#define EX1_DEFINES_H


#define CLIENT_WARMUP_MSG_AMOUNT  (150) // This is what brought us the best throughput,
										// though anything between [0, 200] has given us the same throughput
#define CLIENT_MSG_AMOUNT  (10000)		// We wanted to normalize and smooth the results using an enormous amount of
										// packets to send for each size, normalizing network hiccups.
#define EXP_AMOUNT (10)
#define PORT (7775)
#define MESSAGE_LENGTH (2000)

#endif //EX1_DEFINES_H
