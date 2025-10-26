#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define MAX 100
#define CHECKPOINT_INTERVAL 5

typedef struct {
    char key[50];
    char value[50];
} KV;

KV store[MAX];
int count = 0;

void save_wal(const char *op, const char *key, const char *val) {
    FILE *f = fopen("wal.log", "a");
    if (!f) return;
    fprintf(f, "%s %s %s\n", op, key, val ? val : "");
    fclose(f);
}

void save_checkpoint() {
    FILE *f = fopen("checkpoint.txt", "w");
    if (!f) return;
    for (int i = 0; i < count; i++)
        fprintf(f, "%s %s\n", store[i].key, store[i].value);
    fclose(f);
    fopen("wal.log", "w"); // clear WAL
}

void recover() {
    FILE *f = fopen("checkpoint.txt", "r");
    if (f) {
        while (fscanf(f, "%s %s", store[count].key, store[count].value) == 2)
            count++;
        fclose(f);
    }

    f = fopen("wal.log", "r");
    if (f) {
        char op[10], k[50], v[50];
        while (fscanf(f, "%s %s %s", op, k, v) >= 2) {
            if (strcmp(op, "SET") == 0) {
                int found = 0;
                for (int i = 0; i < count; i++) {
                    if (strcmp(store[i].key, k) == 0) {
                        strcpy(store[i].value, v);
                        found = 1;
                    }
                }
                if (!found && count < MAX) {
                    strcpy(store[count].key, k);
                    strcpy(store[count].value, v);
                    count++;
                }
            } else if (strcmp(op, "DEL") == 0) {
                for (int i = 0; i < count; i++) {
                    if (strcmp(store[i].key, k) == 0) {
                        store[i] = store[count - 1];
                        count--;
                        break;
                    }
                }
            }
        }
        fclose(f);
    }
}

void set(const char *key, const char *val) {
    save_wal("SET", key, val);
    for (int i = 0; i < count; i++)
        if (strcmp(store[i].key, key) == 0) { strcpy(store[i].value, val); return; }
    strcpy(store[count].key, key);
    strcpy(store[count].value, val);
    count++;
}

void del(const char *key) {
    save_wal("DEL", key, "");
    for (int i = 0; i < count; i++)
        if (strcmp(store[i].key, key) == 0) { store[i] = store[count - 1]; count--; return; }
}

void get(const char *key) {
    for (int i = 0; i < count; i++)
        if (strcmp(store[i].key, key) == 0) { printf("%s\n", store[i].value); return; }
    printf("(nil)\n");
}

int main() {
    recover();
    printf("Recovered %d items.\n", count);
    char cmd[10], k[50], v[50];
    int writes = 0;

    while (1) {
        printf("> ");
        if (scanf("%s", cmd) != 1) break;
        if (!strcmp(cmd, "SET")) { scanf("%s %s", k, v); set(k, v); writes++; }
        else if (!strcmp(cmd, "GET")) { scanf("%s", k); get(k); }
        else if (!strcmp(cmd, "DEL")) { scanf("%s", k); del(k); writes++; }
        else if (!strcmp(cmd, "EXIT")) break;
        else printf("Commands: SET, GET, DEL, EXIT\n");

        if (writes >= CHECKPOINT_INTERVAL) {
            save_checkpoint();
            writes = 0;
            printf("Checkpoint saved.\n");
        }
    }
    return 0;
}

