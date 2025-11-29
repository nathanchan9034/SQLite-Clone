/* Imports */
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <assert.h>

typedef struct InputBuffer_Struct{
    char* buffer;
    size_t buffer_length;
    ssize_t input_length;
} InputBuffer;

typedef enum {
    EXECUTE_SUCCESS,
    EXECUTE_DUPLICATE_KEY,
    EXECUTE_TABLE_FULL
} ExecuteResult;

typedef enum {
    META_COMMAND_SUCCESS,
    META_COMMAND_UNRECOGNIZED
} MetaCommandResult;

typedef enum {
    PREPARE_SUCCESS,
    PREPARE_NEGATIVE_ID,
    PREPARE_STRING_TOO_LONG,
    PREPARE_UNRECOGNIZED,
    PREPARE_SYNTAX_ERROR
} PrepareResult;

typedef enum {
    STATEMENT_INSERT,
    STATEMENT_SELECT
} StatementType;

typedef enum {
    NODE_INTERNAL,
    NODE_LEAF
} NodeType;

#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255

typedef struct Row_Struct{
    uint32_t id;
    char username[COLUMN_USERNAME_SIZE + 1];
    char email[COLUMN_EMAIL_SIZE + 1];
} Row;

typedef struct Statement_Struct{
    StatementType type;
    Row row;
} Statement;

#define size_of_attribute(Struct, Attribute) sizeof(((Struct*)0)->Attribute)

#define TABLE_MAX_PAGES 100
#define INVALID_PAGE_NUM UINT32_MAX
const uint32_t PAGE_SIZE = 4096;

const uint32_t ID_SIZE = size_of_attribute(Row, id);
const uint32_t USERNAME_SIZE = size_of_attribute(Row, username);
const uint32_t EMAIL_SIZE = size_of_attribute(Row, email);
const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

/* Node Header Layout */
const uint32_t NODE_TYPE_SIZE = sizeof(uint8_t);
const uint32_t NODE_TYPE_OFFSET = 0;
const uint32_t IS_ROOT_SIZE = sizeof(uint8_t);
const uint32_t IS_ROOT_OFFSET = NODE_TYPE_SIZE;
const uint32_t PARENT_POINTER_SIZE = sizeof(uint32_t);
const uint32_t PARENT_POINTER_OFFSET = IS_ROOT_OFFSET + IS_ROOT_SIZE;
const uint8_t COMMON_NODE_HEADER_SIZE = NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE;

/* Leaf Node Header Layout */
const uint32_t LEAF_NODE_NUM_CELLS_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_NUM_CELLS_OFFSET = COMMON_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_NEXT_LEAF_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_NEXT_LEAF_OFFSET = LEAF_NODE_NUM_CELLS_OFFSET + LEAF_NODE_NUM_CELLS_SIZE;
const uint32_t LEAF_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE + LEAF_NODE_NEXT_LEAF_SIZE;

/* Leaf Node Body Layout */
const uint32_t LEAF_NODE_KEY_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_KEY_OFFSET = 0;
const uint32_t LEAF_NODE_VALUE_SIZE = ROW_SIZE;
const uint32_t LEAF_NODE_VALUE_OFFSET = LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE;
const uint32_t LEAF_NODE_CELL_SIZE = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
const uint32_t LEAF_NODE_SPACE_FOR_CELLS = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_MAX_CELLS = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;
const uint32_t LEAF_NODE_RIGHT_SPLIT_COUNT = (LEAF_NODE_MAX_CELLS + 1) / 2;
const uint32_t LEAF_NODE_LEFT_SPLIT_COUNT = (LEAF_NODE_MAX_CELLS + 1) - LEAF_NODE_RIGHT_SPLIT_COUNT;

/* Internal Node Header Layout */
const uint32_t INTERNAL_NODE_NUM_KEYS_SIZE   = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_NUM_KEYS_OFFSET = COMMON_NODE_HEADER_SIZE;

const uint32_t INTERNAL_NODE_RIGHT_CHILD_SIZE   = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_RIGHT_CHILD_OFFSET =
    INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE;
const uint32_t INTERNAL_NODE_HEADER_SIZE =
    INTERNAL_NODE_NUM_KEYS_SIZE + INTERNAL_NODE_RIGHT_CHILD_SIZE + COMMON_NODE_HEADER_SIZE;

/* Internal Node Body Layout */
const uint32_t INTERNAL_NODE_KEY_SIZE   = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_CHILD_SIZE = sizeof(uint32_t);
const uint32_t INTERNAL_NODE_CELL_SIZE =
    INTERNAL_NODE_CHILD_SIZE + INTERNAL_NODE_KEY_SIZE;
const uint32_t INTERNAL_NODE_MAX_CELLS = 3;


typedef struct Pager_Struct {
    int file_descriptor;
    uint32_t file_length;
    uint32_t num_pages;
    void* pages[TABLE_MAX_PAGES];
} Pager;

typedef struct Table_Struct {
    uint32_t root_page_num;
    Pager* pager;
} Table;

typedef struct Cursor_Struct {
    Table *table;
    uint32_t page_num;
    uint32_t cell_num;
    bool end_of_table;
} Cursor;

void internal_node_split_and_insert(Table* table, uint32_t parent_page_num, uint32_t child_page_num);
void internal_node_insert(Table* table, uint32_t parent_page_num, uint32_t child_page_num);

uint32_t* leaf_node_num_cells(void* node){
    return node + LEAF_NODE_NUM_CELLS_OFFSET;
}

void* leaf_node_cell(void* node, uint32_t cell_num) {
    return node + LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE;
}

uint32_t* leaf_node_key(void* node, uint32_t cell_num){
    return leaf_node_cell(node, cell_num);
}

void* leaf_node_value(void* node, uint32_t cell_num){
    return leaf_node_cell(node, cell_num) + LEAF_NODE_KEY_SIZE;
}

uint32_t* leaf_node_next_leaf(void* node){
    return node + LEAF_NODE_NEXT_LEAF_OFFSET;
}

NodeType get_node_type(void* node){
    uint8_t value = *((uint8_t*)(node + NODE_TYPE_OFFSET));
    return (NodeType)value;
}

void set_node_type(void* node, NodeType type){
    uint8_t value = type;
    *((uint8_t*)(node + NODE_TYPE_OFFSET)) = value;
}

uint32_t* internal_node_num_keys(void* node){ return node + INTERNAL_NODE_NUM_KEYS_OFFSET; }

uint32_t* internal_node_right_child(void* node){ return node + INTERNAL_NODE_RIGHT_CHILD_OFFSET; }

uint32_t* internal_node_cell(void* node, uint32_t cell_num){ return node + INTERNAL_NODE_HEADER_SIZE + cell_num * INTERNAL_NODE_CELL_SIZE; }

uint32_t* internal_node_child(void* node, uint32_t child_num){
    uint32_t num_keys = *internal_node_num_keys(node);

    if (child_num > num_keys){
        printf("Tried to access child num %d > num keys %d\n", child_num, num_keys);
        exit(0);
    } else if (child_num == num_keys){
        uint32_t* right_child = internal_node_right_child(node);

        if (*right_child == INVALID_PAGE_NUM){
            printf("Tried to access right child of node, but was invalid page\n");
            exit(1);
        }

        return right_child;
    } else {
        uint32_t* child = internal_node_cell(node, child_num);

        if (*child == INVALID_PAGE_NUM){
            printf("Tried to access child of node %d, but was invalid page\n", child_num);
            exit(0);
        }

        return child;
    }
}

uint32_t* internal_node_key(void* node, uint32_t key_num){ return (void*) internal_node_cell(node, key_num) + INTERNAL_NODE_CHILD_SIZE; }

bool is_node_root(void* node){
    uint8_t value = *((uint8_t*)(node + IS_ROOT_OFFSET));
    return (bool)value;
}

void set_node_root(void* node, bool is_root){
    uint8_t value = is_root;
    *((uint8_t*)(node + IS_ROOT_OFFSET)) = value;
}

uint32_t* node_parent(void* node){
    return node + PARENT_POINTER_OFFSET;
}

void* get_page(Pager* pager, uint32_t page_num){
    if (page_num > TABLE_MAX_PAGES){
        printf("Tried to fetch page number out of bounds. %d > %d\n", page_num, TABLE_MAX_PAGES);
        exit(0);
    }

    if (pager->pages[page_num] == NULL){
        void* page = malloc(PAGE_SIZE);
        uint32_t num_pages = pager->file_length / PAGE_SIZE;

        if (pager->file_length % PAGE_SIZE){
            num_pages += 1;
        }

        if (page_num <= num_pages){
            lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
            ssize_t bytes = read(pager->file_descriptor, page, PAGE_SIZE);

            if (bytes == -1){
                printf("Error reading from file: %d\n", errno);
                exit(0);
            }
        }

        pager->pages[page_num] = page;

        if (page_num >= pager->num_pages){
            pager->num_pages = page_num + 1;
        }
    }

    return pager->pages[page_num];
}

uint32_t get_node_max_key(Pager* pager, void* node){
    if (get_node_type(node) == NODE_LEAF){
        uint32_t num_cells = *leaf_node_num_cells(node);

        if (num_cells == 0){
            return 0;
        }

        return *leaf_node_key(node, num_cells - 1);
    }

    void* right_child = get_page(pager, *internal_node_right_child(node));
    return get_node_max_key(pager, right_child);
}

Cursor* leaf_node_find(Table* table, uint32_t page_num, uint32_t key){
    void* node = get_page(table->pager, page_num);
    uint32_t num_cells = *leaf_node_num_cells(node);

    Cursor* cursor = malloc(sizeof(Cursor));
    cursor->table = table;
    cursor->page_num = page_num;

    /* Perform a binary search to find the leaf node */
    uint32_t min = 0;
    uint32_t max = num_cells;

    while (max != min){
        uint32_t index = (min + max) / 2;
        uint32_t key_at_index = *leaf_node_key(node, index);

        if (key == key_at_index){
            cursor->cell_num = index;
            return cursor;
        } 
        
        if (key < key_at_index){
            max = index;
        } else {
            min = index + 1;
        }
    }

    cursor->cell_num = min;
    return cursor;
}

uint32_t internal_node_find_child(void* node, uint32_t key){
    /* Return the index of the child which should contain the given key */
    uint32_t num_keys = *internal_node_num_keys(node);

    /* Binary Search */
    uint32_t min = 0;
    uint32_t max = num_keys;

    while (min != max){
        uint32_t mid = (min + max) / 2;
        uint32_t key_to_right = *internal_node_key(node, mid);

        if (key_to_right >= key){
            max = mid;
        } else {
            min = mid + 1;
        }
    }

    return min;
}

Cursor* internal_node_find(Table* table, uint32_t page_num, uint32_t key){
    void* node = get_page(table->pager, page_num);

    uint32_t child_index = internal_node_find_child(node, key);
    uint32_t child_num = *internal_node_child(node, child_index);
    void* child = get_page(table->pager, child_num);

    switch(get_node_type(child)){
        case NODE_INTERNAL:
            return internal_node_find(table, child_num, key);
        case NODE_LEAF:
            return leaf_node_find(table, child_num, key);
    }
}

void update_internal_node_key(void* node, uint32_t old_key, uint32_t new_key){
    uint32_t old_child_index = internal_node_find_child(node, old_key);
    *internal_node_key(node, old_child_index) = new_key;
}

void init_internal_node(void* node){
    set_node_type(node, NODE_INTERNAL);
    set_node_root(node, false);
    *internal_node_num_keys(node) = 0;

    *internal_node_right_child(node) = INVALID_PAGE_NUM;
}

void init_leaf_node(void* node){
    set_node_type(node, NODE_LEAF);
    set_node_root(node, false);
    *leaf_node_num_cells(node) = 0;
    *leaf_node_next_leaf(node) = INVALID_PAGE_NUM;
}

void print_row(Row* row){
    printf("(%d, %s, %s)\n", row->id, row->username, row->email);
}

void serialize_row(Row* row, void* dest){
    memcpy(dest + ID_OFFSET, &(row->id), ID_SIZE);
    strncpy(dest + USERNAME_OFFSET, row->username, USERNAME_SIZE);
    strncpy(dest + EMAIL_OFFSET, row->email, EMAIL_SIZE);
}

void deserialize_row(void* source, Row* dest){
    memcpy(&(dest->id), source + ID_OFFSET, ID_SIZE);
    memcpy(&(dest->username), source + USERNAME_OFFSET, USERNAME_SIZE);
    memcpy(&(dest->email), source + EMAIL_OFFSET, EMAIL_SIZE);
}

Cursor* table_find(Table* table, uint32_t key){
    uint32_t root_page_num = table->root_page_num;
    void* root_node = get_page(table->pager, root_page_num);

    if (get_node_type(root_node) == NODE_LEAF){
        return leaf_node_find(table, root_page_num, key);
    } else {
        return internal_node_find(table, root_page_num, key);
    }
}

Cursor* table_start(Table* table){
    Cursor* cursor = table_find(table, 0);

    void* node = get_page(table->pager, cursor->page_num);
    uint32_t num_cells = *leaf_node_num_cells(node);
    cursor->end_of_table = (num_cells == 0);

    return cursor;
}

void* cursor_value(Cursor* cursor){
    uint32_t page_num = cursor->page_num;
    void* page = get_page(cursor->table->pager, page_num);
    
    return leaf_node_value(page, cursor->cell_num);
}

void cursor_advance(Cursor* cursor){
    uint32_t page_num = cursor->page_num;
    void* node = get_page(cursor->table->pager, page_num);
    cursor->cell_num += 1;

    if (cursor->cell_num >= (*leaf_node_num_cells(node))){
        uint32_t next_page_num = *leaf_node_next_leaf(node);
        if (next_page_num == 0){
            cursor->end_of_table = true;
        } else {
            cursor->page_num = next_page_num;
            cursor->cell_num = 0;
        }
    }
}

Pager* pager_open(const char* filename){
    int fd = open(filename, O_RDWR | O_CREAT, S_IWUSR | S_IRUSR);

    if (fd == -1){
        printf("Unable to open file\n");
        exit(0);
    }

    off_t file_length = lseek(fd, 0, SEEK_END);

    Pager* pager = malloc(sizeof(Pager));
    pager->file_descriptor = fd;
    pager->file_length = file_length;
    pager->num_pages = (file_length / PAGE_SIZE);

    if (file_length % PAGE_SIZE != 0){
        printf("Db file is not a whole number of pages. Corrupt file.\n");
        exit(0);
    }

    for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++){
        pager->pages[i] = NULL;
    }

    return pager;
}

Table* db_open(const char* filename){
    Pager* pager = pager_open(filename);

    Table* table = (Table*) malloc(sizeof(table));
    table->pager = pager;
    table->root_page_num = 0;

    if (pager->num_pages == 0){
        void* root_node = get_page(pager, 0);
        init_leaf_node(root_node);
        set_node_root(root_node, true);
    }

    return table;
}

InputBuffer* init_input_buffer(){
    InputBuffer* ib = (InputBuffer*) malloc(sizeof(InputBuffer));
    ib->buffer = NULL;
    ib->buffer_length = 0;
    ib->input_length = 0;

    return ib;
}

void print_prompt() {
    printf("db > ");
}

void read_input(InputBuffer* ib){
    ssize_t bytes = getline(&(ib->buffer), &(ib->buffer_length), stdin);

    if (bytes <= 0){
        printf("Error, please try again");
        exit(0);
    }

    ib->input_length = bytes - 1;
    ib->buffer[bytes - 1] = 0;
}

void close_input_buffer(InputBuffer* ib){
    free(ib->buffer);
    free(ib);
}

void pager_flush(Pager* pager, uint32_t page_num){
    if (pager->pages[page_num] == NULL){
        printf("Tried to flush null page.\n");
        exit(0);
    }

    off_t offset = lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);

    if (offset == -1){
        printf("Error seeking.\n");
        exit(0);
    }

    ssize_t bytes = write(pager->file_descriptor, pager->pages[page_num], PAGE_SIZE);
    
    if (bytes == -1){
        printf("Error writing.\n");
        exit(0);
    }
}

void db_close(Table* table){
    Pager* pager = table->pager;

    for (uint32_t i = 0; i < pager->num_pages; i++){
        if (pager->pages[i] == NULL){
            continue;
        }

        pager_flush(pager, i);
        free(pager->pages[i]);
        pager->pages[i] = NULL;
    }

    int res = close(pager->file_descriptor);

    if (res == -1){
        printf("Error closing the db file.\n");
        exit(0);
    }

    for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++){
        void* page = pager->pages[i];
        if (page) {
            free(page);
            pager->pages[i] = NULL;
        }
    }

    free(pager);
    free(table);
}

void print_constants(){
    printf("Constants:\n");
    printf("ROW_SIZE: %d\n", ROW_SIZE);
    printf("COMMON_NODE_HEADER_SIZE: %d\n", COMMON_NODE_HEADER_SIZE);
    printf("LEAF_NODE_HEADER_SIZE: %d\n", LEAF_NODE_HEADER_SIZE);
    printf("LEAF_NODE_CELL_SIZE: %d\n", LEAF_NODE_CELL_SIZE);
    printf("LEAF_NODE_SPACE_FOR_CELLS: %d\n", LEAF_NODE_SPACE_FOR_CELLS);
    printf("LEAF_NODE_MAX_CELLS: %d\n", LEAF_NODE_MAX_CELLS);
}

void indent(uint32_t level){
    for (uint32_t i = 0; i < level; i++){
        printf("   ");
    }
}

void print_tree(Pager* pager, uint32_t page_num, uint32_t indentation_level){
    void* node = get_page(pager, page_num);
    uint32_t num_keys, child;

    switch(get_node_type(node)){
        case(NODE_LEAF):
            num_keys = *leaf_node_num_cells(node);
            indent(indentation_level);
            printf("- leaf (size %d)\n", num_keys);

            for (uint32_t i = 0; i < num_keys; i++){
                indent(indentation_level + 1);
                printf("- %d\n", *leaf_node_key(node, i));
            }

            break;
        case(NODE_INTERNAL):
            num_keys = *internal_node_num_keys(node);
            indent(indentation_level);
            printf("- internal (size %d)\n", num_keys);

            if (num_keys > 0){
                for (uint32_t i = 0; i < num_keys; i++){
                    child = *internal_node_child(node, i);
                    print_tree(pager, child, indentation_level + 1);

                    indent(indentation_level + 1);
                    printf("- key %d\n", *internal_node_key(node, i));
                }

                child = *internal_node_right_child(node);
                print_tree(pager, child, indentation_level + 1);
            }

            break;
    }
}

MetaCommandResult perform_meta_command(InputBuffer* ib, Table* table){
    if (strcmp(ib->buffer, ".exit") == 0){
        db_close(table);
        exit(0);
    } else if (strcmp(ib->buffer, ".btree") == 0){
        print_tree(table->pager, 0, 0);
        return META_COMMAND_SUCCESS;
    } else if (strcmp(ib->buffer, ".constants") == 0){
        print_constants();
        return META_COMMAND_SUCCESS;
    } else {
        return META_COMMAND_UNRECOGNIZED;
    }
}

PrepareResult prepare_insert(InputBuffer* ib, Statement* statement){
    statement->type = STATEMENT_INSERT;

    char* keyword = strtok(ib->buffer, " ");
    char* id_string = strtok(NULL, " ");
    char* username = strtok(NULL, " ");
    char* email = strtok(NULL, " ");

    if (id_string == NULL || username == NULL || email == NULL){
        return PREPARE_SYNTAX_ERROR;
    }

    int id = atoi(id_string);

    if (id < 0){
        return PREPARE_NEGATIVE_ID;
    }

    if (strlen(username) > COLUMN_USERNAME_SIZE){
        return PREPARE_STRING_TOO_LONG;
    }

    if (strlen(email) > COLUMN_EMAIL_SIZE){
        return PREPARE_STRING_TOO_LONG;
    }

    statement->row.id = id;
    strcpy(statement->row.username, username);
    strcpy(statement->row.email, email);

    return PREPARE_SUCCESS;
}

PrepareResult prepare_statement(InputBuffer* ib, Statement* statement){
    if (strncmp(ib->buffer, "insert", 6) == 0){
        return prepare_insert(ib, statement);
    }

    if (strcmp(ib->buffer, "select") == 0){
        statement->type = STATEMENT_SELECT;

        return PREPARE_SUCCESS;
    }

    return PREPARE_UNRECOGNIZED;
}

uint32_t get_unused_page_num(Pager* pager){
    uint32_t unused = pager->num_pages;
    pager->num_pages += 1;
    return unused;
}

void create_new_root(Table* table, uint32_t right_child_page_num){
    /* Handle splitting the root
       Old root is copied to the new page and becomes the left child
       Address of the right child is passed in 
       Re-initialize root page to contain the new root node
       New root node points to two children */
    void* root = get_page(table->pager, table->root_page_num);
    void* right_child = get_page(table->pager, right_child_page_num);
    uint32_t left_child_page_num = get_unused_page_num(table->pager);
    void* left_child = get_page(table->pager, left_child_page_num);

    if (get_node_type(root) == NODE_INTERNAL){
        init_internal_node(right_child);
        init_internal_node(left_child);
    }

    /* Left child has data copied from old root */
    memcpy(left_child, root, PAGE_SIZE);
    set_node_root(left_child, false);

    if (get_node_type(left_child) == NODE_INTERNAL){
        void* child;

        for (int i = 0; i < *internal_node_num_keys(left_child); i++){
            child = get_page(table->pager, *internal_node_child(left_child, i));
            *node_parent(child) = left_child_page_num;    
        }
    }

    /* Root node is a new internal node with one key and two children */
    init_internal_node(root);
    set_node_root(root, true);
    *internal_node_num_keys(root) = 1;
    *internal_node_child(root, 0) = left_child_page_num;
    uint32_t left_child_max_key = get_node_max_key(table->pager, left_child);
    *internal_node_key(root, 0) = left_child_max_key;
    *internal_node_right_child(root) = right_child_page_num;
    *node_parent(left_child) = table->root_page_num;
    *node_parent(right_child) =table->root_page_num;
}

void internal_node_split_and_insert(Table* table, uint32_t parent_page_num, uint32_t child_page_num) {
  uint32_t old_page_num = parent_page_num;
  void* old_node = get_page(table->pager,parent_page_num);
  uint32_t old_max = get_node_max_key(table->pager, old_node);

  void* child = get_page(table->pager, child_page_num); 
  uint32_t child_max = get_node_max_key(table->pager, child);
  uint32_t new_page_num = get_unused_page_num(table->pager);
  uint32_t splitting_root = is_node_root(old_node);

  void* parent;
  void* new_node;
  if (splitting_root) {
    create_new_root(table, new_page_num);
    parent = get_page(table->pager,table->root_page_num);

    /* If we are splitting the root, we need to update old_node to point
    to the new root's left child, new_page_num will already point to
    the new root's right child */
    old_page_num = *internal_node_child(parent,0);
    old_node = get_page(table->pager, old_page_num);
  } else {
    parent = get_page(table->pager,*node_parent(old_node));
    new_node = get_page(table->pager, new_page_num);
    init_internal_node(new_node);
  }
  
  uint32_t* old_num_keys = internal_node_num_keys(old_node);

  uint32_t cur_page_num = *internal_node_right_child(old_node);
  void* cur = get_page(table->pager, cur_page_num);

  /* First put right child into new node and set right child of old node to invalid page number */
  internal_node_insert(table, new_page_num, cur_page_num);
  *node_parent(cur) = new_page_num;
  *internal_node_right_child(old_node) = INVALID_PAGE_NUM;

  /* For each key until you get to the middle key, move the key and the child to the new node */
  for (int i = INTERNAL_NODE_MAX_CELLS - 1; i > INTERNAL_NODE_MAX_CELLS / 2; i--) {
    cur_page_num = *internal_node_child(old_node, i);
    cur = get_page(table->pager, cur_page_num);

    internal_node_insert(table, new_page_num, cur_page_num);
    *node_parent(cur) = new_page_num;

    (*old_num_keys)--;
  }

  /* Set child before middle key, which is now the highest key, to be node's right child, and decrement number of keys */
  *internal_node_right_child(old_node) = *internal_node_child(old_node,*old_num_keys - 1);
  (*old_num_keys)--;

  /* Determine which of the two nodes after the split should contain the child to be inserted, and insert the child */
  uint32_t max_after_split = get_node_max_key(table->pager, old_node);

  uint32_t destination_page_num = child_max < max_after_split ? old_page_num : new_page_num;

  internal_node_insert(table, destination_page_num, child_page_num);
  *node_parent(child) = destination_page_num;

  update_internal_node_key(parent, old_max, get_node_max_key(table->pager, old_node));

  if (!splitting_root) {
    internal_node_insert(table,*node_parent(old_node),new_page_num);
    *node_parent(new_node) = *node_parent(old_node);
  }
}

void internal_node_insert(Table* table, uint32_t parent_page_num, uint32_t child_page_num){
    /* Add a new child/key pair to parent that corresponds to child*/

    void* parent = get_page(table->pager, parent_page_num);
    void* child = get_page(table->pager, child_page_num);

    uint32_t child_max_key = get_node_max_key(table->pager, child);
    uint32_t index = internal_node_find_child(parent, child_max_key);

    uint32_t original_num_keys = *internal_node_num_keys(parent);

    if (original_num_keys >= INTERNAL_NODE_MAX_CELLS){
        internal_node_split_and_insert(table, parent_page_num, child_page_num);
        return;
    }

    uint32_t right_child_page_num = *internal_node_right_child(parent);

    /* An internal node with a right child of INVALID_PAGE_NUM is empty */
    if (right_child_page_num == INVALID_PAGE_NUM){
        *internal_node_right_child(parent) = child_page_num;
        *node_parent(child) = parent_page_num;
        return;
    }

    void* right_child = get_page(table->pager, right_child_page_num);

    /* If we are already at the max number of cells for a node, we cannot increment before 
       splitting. Incrementing without inserting a new key/child pair and immediately
       calling internal_node_split_and_insert() has the effect of creating a new key at 
       (max_cells + 1) with an uninitialized value */
    *internal_node_num_keys(parent) = original_num_keys + 1;

    if (child_max_key > get_node_max_key(table->pager, right_child)){
        *internal_node_child(parent, original_num_keys) = right_child_page_num;
        *internal_node_key(parent, original_num_keys) = get_node_max_key(table->pager, right_child);
        *internal_node_right_child(parent) = child_page_num;
        *node_parent(child) = parent_page_num;
        *node_parent(right_child) = parent_page_num;
    } else {
        for (uint32_t i = original_num_keys; i > index; i--){
            void* dest = internal_node_cell(parent, i);
            void* source = internal_node_cell(parent, i - 1);
            memcpy(dest, source, INTERNAL_NODE_CELL_SIZE);
        }

        *internal_node_child(parent, index) = child_page_num;
        *internal_node_key(parent, index) = child_max_key;
        *node_parent(child) = parent_page_num;
    }
}

void leaf_node_split_and_insert (Cursor* cursor, uint32_t key, Row* value){
    /* Create a new node and move half the cells over
       Insert the new value in one of the two nodes
       Update parent or create a new paren */

    void* old = get_page(cursor->table->pager, cursor->page_num);
    uint32_t old_num_cells = *leaf_node_num_cells(old);
    uint32_t old_max = get_node_max_key(cursor->table->pager, old);

    uint32_t new_page_num = get_unused_page_num(cursor->table->pager);

    assert(old_num_cells == LEAF_NODE_MAX_CELLS);
    assert(cursor->cell_num <= old_num_cells);

    void* new = get_page(cursor->table->pager, new_page_num);
    init_leaf_node(new);
    *node_parent(new) = *node_parent(old);

    *leaf_node_next_leaf(new) = *leaf_node_next_leaf(old);
    *leaf_node_next_leaf(old) = new_page_num;

    /* All existing keys plus new key should be divided 
        evenly betweenthe old and new nodes. Starting from
        the right, move each key to the correct position */
    for (int32_t i = LEAF_NODE_MAX_CELLS; i >= 0; i--){
        void* dest_node;

        if (i >= LEAF_NODE_LEFT_SPLIT_COUNT){
            dest_node = new;
        } else {
            dest_node = old;
        }

        uint32_t index_within_node = i % LEAF_NODE_LEFT_SPLIT_COUNT;
        void* dest = leaf_node_cell(dest_node, index_within_node);

        if (i == cursor->cell_num){
            serialize_row(value, leaf_node_value(dest_node, index_within_node));
            *leaf_node_key(dest_node, index_within_node) = key;
        } else if (i > cursor->cell_num){
            memmove(dest, leaf_node_cell(old, i - 1), LEAF_NODE_CELL_SIZE);
        } else {
            memmove(dest, leaf_node_cell(old, i), LEAF_NODE_CELL_SIZE);
        }
    }

    /* Update cell count on both leaf nodes */
    *(leaf_node_num_cells(old)) = LEAF_NODE_LEFT_SPLIT_COUNT;
    *(leaf_node_num_cells(new)) = LEAF_NODE_RIGHT_SPLIT_COUNT;

    /* Update the nodes' parents. If the original node was the 
        root, it had no parents. In that case, create a new root
        node to act as the parent */
    if (is_node_root(old)){
        return create_new_root(cursor->table, new_page_num);
    } else {
        uint32_t parent_page_num = *node_parent(old);
        uint32_t new_max = get_node_max_key(cursor->table->pager, old);
        void* parent = get_page(cursor->table->pager, parent_page_num);

        update_internal_node_key(parent, old_max, new_max);
        internal_node_insert(cursor->table, parent_page_num, new_page_num);
        return;
    }
}

void leaf_node_insert(Cursor* cursor, uint32_t key, Row* value){
    void* node = get_page(cursor->table->pager, cursor->page_num);

    uint32_t num_cells = *leaf_node_num_cells(node);

    assert(num_cells <= LEAF_NODE_MAX_CELLS);
    assert(cursor->cell_num <= num_cells);

    if (num_cells >= LEAF_NODE_MAX_CELLS){
        leaf_node_split_and_insert(cursor, key, value);
        return;
    }

    if (cursor->cell_num < num_cells){
        for (uint32_t i = num_cells; i > cursor->cell_num; i--){
            memmove(leaf_node_cell(node, i), leaf_node_cell(node, i - 1), LEAF_NODE_CELL_SIZE);
        }
    }

    *(leaf_node_num_cells(node)) += 1;
    *(leaf_node_key(node, cursor->cell_num))= key;
    serialize_row(value, leaf_node_value(node, cursor->cell_num));
}

ExecuteResult execute_insert(Statement* statement, Table* table){
    void* node = get_page(table->pager, table->root_page_num);
    uint32_t num_cells = (*leaf_node_num_cells(node));

    Row* row = &(statement->row);
    uint32_t key_to_insert = row->id;
    Cursor* cursor = table_find(table, key_to_insert);

    if (cursor->cell_num < num_cells){
        uint32_t key_at_index = *leaf_node_key(node, cursor->cell_num);
        if (key_at_index == key_to_insert){
            return EXECUTE_DUPLICATE_KEY;
        }
    }

    leaf_node_insert(cursor, row->id, row);

    free(cursor);

    return EXECUTE_SUCCESS;
}

ExecuteResult execute_select(Statement* statement, Table* table){
    Row row;
    Cursor* cursor = table_start(table);
    
    while (!(cursor->end_of_table)){
        deserialize_row(cursor_value(cursor), &row);
        print_row(&row);
        cursor_advance(cursor);
    }

    free(cursor);

    return EXECUTE_SUCCESS;
}

ExecuteResult execute_statement(Statement* statement, Table* table){
    switch (statement->type){
        case (STATEMENT_INSERT):
            return execute_insert(statement, table);
        case (STATEMENT_SELECT):
            return execute_select(statement, table);
    }
}

int main(int argc, char* argv[]){
    if (argc < 2){
        printf("Must supply a database filename.\n");
        exit(0);
    }

    char* filename = argv[1];
    Table* table = db_open(filename);
    InputBuffer* ib = init_input_buffer();

    while (1){
        print_prompt();
        read_input(ib);

        if (ib->buffer[0] == '.'){
            switch(perform_meta_command(ib, table)){
                case (META_COMMAND_SUCCESS):
                    continue;
                case (META_COMMAND_UNRECOGNIZED):
                    printf("Unrecognized command '%s'.\n", ib->buffer);
                    continue;
            }
        }

        Statement statement;

        switch (prepare_statement(ib, &statement)){
            case (PREPARE_SUCCESS):
                break;
            case (PREPARE_NEGATIVE_ID):
                printf("ID must be positive.\n");
                continue;
            case (PREPARE_STRING_TOO_LONG):
                printf("String is too long.\n");
                continue;
            case (PREPARE_SYNTAX_ERROR):
                printf("Syntax error. Could not parse statement.\n");
                continue;
            case (PREPARE_UNRECOGNIZED):
                printf("Unrecognized keyword at start of '%s'.\n", ib->buffer);
                continue;
        }

        switch(execute_statement(&statement, table)){
            case (EXECUTE_SUCCESS):
                printf("Executed.\n");
                break;
            case (EXECUTE_DUPLICATE_KEY):
                printf("Error: Duplicate key.\n");
                break;
            case (EXECUTE_TABLE_FULL):
                printf("Error: Table is full");
                break;
        }
    }
}