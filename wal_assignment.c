#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define MAX 100

typedef struct {
    char key[50];
    char value[50];
} KV;

KV db[MAX];
int count = 0;

/* Load checkpoint data (base state) */
void load_checkpoint() {
    FILE *f = fopen("checkpoint.txt", "r");
    if (!f) return;  // no checkpoint yet
    while (fscanf(f, "%s %s", db[count].key, db[count].value) == 2)
        count++;
    fclose(f);
}

/* Apply WAL updates (recent operations after checkpoint) */
void apply_wal() {
    FILE *f = fopen("wal.log", "r");
    if (!f) return;  // no WAL found
    char op[10], k[50], v[50];

    while (fscanf(f, "%s %s %s", op, k, v) >= 2) {
        if (strcmp(op, "SET") == 0) {
            int found = 0;
            for (int i = 0; i < count; i++) {
                if (strcmp(db[i].key, k) == 0) {
                    strcpy(db[i].value, v);
                    found = 1;
                    break;
                }
            }
            if (!found && count < MAX) {
                strcpy(db[count].key, k);
                strcpy(db[count].value, v);
                count++;
            }
        } else if (strcmp(op, "DEL") == 0) {
            for (int i = 0; i < count; i++) {
                if (strcmp(db[i].key, k) == 0) {
                    db[i] = db[count - 1];  // replace with last
                    count--;
                    break;
                }
            }
        }
    }
    fclose(f);
}

int main() {
    printf("Starting recovery process...\n");

    load_checkpoint();  // Step 1: restore checkpoint
    apply_wal();        // Step 2: replay WAL

    printf("\nRecovered database state:\n");
    for (int i = 0; i < count; i++)
        printf("  %s = %s\n", db[i].key, db[i].value);

    if (count == 0)
        printf("  (empty)\n");

    return 0;
}

