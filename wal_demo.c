#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>

#define WAL_FILE "wal.log"
#define DB_FILE  "db.txt"
#define MAX_LINE 256

// Write helper: append line to file with optional fsync
void append_file(const char *filename, const char *line, int sync) {
    int fd = open(filename, O_WRONLY | O_APPEND | O_CREAT, 0644);
    if (fd < 0) { perror("open"); exit(1); }

    char buf[MAX_LINE];
    int len = snprintf(buf, MAX_LINE, "%s\n", line);

    if (write(fd, buf, len) != len) {
        perror("write");
        close(fd);
        exit(1);
    }

    if (sync) fsync(fd);
    close(fd);
}

// Write transaction without fsync
void write_nosync(const char *key, const char *value) {
    append_file(WAL_FILE, "TRANSACTION 1 BEGIN", 0);
    char buf[MAX_LINE];
    snprintf(buf, MAX_LINE, "SET %s %s", key, value);
    append_file(WAL_FILE, buf, 0);
    append_file(WAL_FILE, "TRANSACTION 1 COMMIT", 0);
    printf("Wrote to WAL without fsync.\n");
}

// Write transaction, fsync WAL, then simulate crash
void crash_after_wal(const char *key, const char *value) {
    append_file(WAL_FILE, "TRANSACTION 1 BEGIN", 1);
    char buf[MAX_LINE];
    snprintf(buf, MAX_LINE, "SET %s %s", key, value);
    append_file(WAL_FILE, buf, 1);
    append_file(WAL_FILE, "TRANSACTION 1 COMMIT", 1);
    printf("WAL written and fsynced. Simulating crash before applying DB.\n");
    exit(1); // simulate crash
}

// Recover committed transactions from WAL
void recover() {
    int fd = open(WAL_FILE, O_RDONLY);
    if (fd < 0) { perror("open wal"); exit(1); }

    char buf[MAX_LINE];
    int n, pos = 0;
    int in_tx = 0;

    while ((n = read(fd, buf + pos, 1)) > 0) {
        if (buf[pos] == '\n') {
            buf[pos] = '\0'; // terminate line

            if (strncmp(buf, "TRANSACTION", 11) == 0 && strstr(buf, "BEGIN"))
                in_tx = 1;
            else if (strncmp(buf, "TRANSACTION", 11) == 0 && strstr(buf, "COMMIT"))
                in_tx = 0;
            else if (in_tx && strncmp(buf, "SET", 3) == 0)
                append_file(DB_FILE, buf, 0);

            pos = 0; // reset buffer for next line
        } else {
            pos++;
            if (pos >= MAX_LINE - 1) pos = MAX_LINE - 2;
        }
    }

    close(fd);
    printf("Recovery complete.\n");
}

// Display WAL and DB
void display() {
    int fd;
    char buf[MAX_LINE];
    int n, pos = 0;

    printf("=== WAL LOG ===\n");
    fd = open(WAL_FILE, O_RDONLY);
    if (fd >= 0) {
        while ((n = read(fd, buf, sizeof(buf))) > 0)
            write(STDOUT_FILENO, buf, n);
        close(fd);
    }

    printf("\n=== DB FILE ===\n");
    fd = open(DB_FILE, O_RDONLY);
    if (fd >= 0) {
        while ((n = read(fd, buf, sizeof(buf))) > 0)
            write(STDOUT_FILENO, buf, n);
        close(fd);
    }
}

int main(int argc, char *argv[]) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <command> [args]\n", argv[0]);
        return 1;
    }

    if (strcmp(argv[1], "write-nosync") == 0 && argc == 4)
        write_nosync(argv[2], argv[3]);
    else if (strcmp(argv[1], "crash-after-wal") == 0 && argc == 4)
        crash_after_wal(argv[2], argv[3]);
    else if (strcmp(argv[1], "recover") == 0)
        recover();
    else if (strcmp(argv[1], "display") == 0)
        display();
    else
        fprintf(stderr, "Invalid command or arguments.\n");

    return 0;
}

