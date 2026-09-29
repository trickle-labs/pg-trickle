#include <stdlib.h>

int main(void) {
    char *value = malloc(1);
    value[1] = 1;
    free(value);
    return 0;
}
