/* =======================================================================
 *   (c) Copyright Hewlett-Packard Development Company, L.P., 2008
 *
 *   This program is free software: you can redistribute it and/or modify
 *   it under the terms of version 2 the GNU General Public License as
 *   published by the Free Software Foundation.
 *   
 *   This program is distributed in the hope that it will be useful,
 *   but WITHOUT ANY WARRANTY; without even the implied warranty of
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *   GNU General Public License for more details.
 *   
 *   You should have received a copy of the GNU General Public License
 *   along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * =======================================================================
 */

/*
 * Copyright (c) 2002-2004 Hewlett-Packard Development Company, L.P.
 */
#ifndef ACTIONS_H
#define ACTIONS_H

/* action structure */
typedef struct action {
        char *action_name;
        int (*action_main)(int, char **);
	void (*action_usage)(void);
} action_t;

/* action mains */
/* int sample_main(int, char **); */
int chfs_main(int, char **);
int chio_main(int, char **);
int list_main(int, char**);
int extend_main(int, char **);
int getattr_main(int, char **);
int mvlog_main(int, char **);
int info_main(int, char **);
int addvol_main(int, char**);
int rmvol_main(int, char**);
int migrate_main(int, char**);
int autotune_main(int, char**);
int scan_main(int, char**);
int defrag_main(int, char**);
int rmfs_main(int, char**);
int balance_main(int, char**);
int multi_main(int, char**);
int rename_main(int, char**);
int prealloc_main(int, char**);
int snap_main(int, char**);
int promote_main(int, char**);
int demote_main(int, char**);
int helper_main(int, char**);

/* action usages */
/* void sample_usage(void); */
void chfs_usage(void);
void chio_usage(void);
void list_usage(void);
void extend_usage(void);
void getattr_usage(void);
void mvlog_usage(void);
void info_usage(void);
void addvol_usage(void);
void rmvol_usage(void);
void migrate_usage(void);
void autotune_usage(void);
void scan_usage(void);
void defrag_usage(void);
void rmfs_usage(void);
void balance_usage(void);
void multi_usage(void);
void rename_usage(void);
void prealloc_usage(void);
void snap_usage(void);
void promote_usage(void);
void demote_usage(void);
void helper_usage(void);

#endif /* ACTIONS_H */
