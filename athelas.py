import streamlit as st
import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import time
import json
import random

# Inject custom CSS for printing
st.markdown("""
    <style>
    @media print {
        /* Hide the sidebar completely */
        [data-testid="stSidebar"] {
            display: none !important;
        }
        
        /* Optional: Hide the top header bar (with the running man/deploy button) */
        header {
            display: none !important;
        }

        /* Expand the main content to full width to use the empty space */
        .main .block-container {
            max-width: 100% !important;
            padding-left: 1rem !important;
            padding-right: 1rem !important;
        }
    }
    </style>
    """, unsafe_allow_html=True)


# --- Configuration & Constants ---
st.set_page_config(page_title="Athelas | KP Care at Home", page_icon="🌿", layout="wide")

DB_FILE = "incidents.db"

# --- Fixed Options ---
SOURCE_CATEGORIES = ["Email", "Chat"]
ISSUE_TYPES = ["Hardware", "Software", "Network", "Access/Permissions", "Workflow", "Training", "Data Error"]
WORKAROUND_OPTIONS = ["Yes", "No", "Pending"]
STATUS_OPTIONS = ["New", "In Progress", "On Hold", "Resolved", "Closed"]
PROJECT_STATUS_OPTIONS = ["Planning", "Active", "On Hold", "Completed", "Cancelled"]
HEALTH_COLORS = {"On Track": "🟢", "At Risk": "🟡", "Off Track": "🔴", "Not Started": "⚪", "Completed": "🔵"}

# Reference Tables
TEAMS = {
    "AOP": "Agency Operations",
    "BPF": "Business Performance",
    "BTS": "Business and Technology Solutions",
    "CAD": "Community Agency Division",
    "CLX": "Clinical Excellence",
    "DME": "Durable Medical Equipment",
    "EXC": "Executives",
    "HHC": "Home Health",
    "HOS": "Hospice",
    "MTS": "Medical Transportation Services",
    "PBI": "Prebilling",
    "RMH": "Referral Management Hub"
}

PROJECT_TYPES = {
    "01": "Operations",
    "02": "Technology",
    "03": "Clinical",
    "04": "Compliance",
    "05": "Finance",
    "06": "Strategy",
    "07": "Training",
    "08": "Data & Analytics",
    "09": "Communications"
}

DEFAULT_BTS_MEMBERS = [
    "Joshua Ay-Ad", "Linda Chow", "Aaron Gunewardena", "Katherine Mollure",
    "Christine Antonio", "Annie Wongkovit", "Carla Santarromana"
]

# --- Database Functions ---

def get_db_connection():
    """Helper to get connection with row factory and timeout for concurrency."""
    conn = sqlite3.connect(DB_FILE, timeout=10)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    """Initialize the SQLite database and handle migrations."""
    conn = get_db_connection()
    c = conn.cursor()

    # 1. Users
    c.execute('''CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL UNIQUE, team TEXT NOT NULL, is_active INTEGER DEFAULT 1, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
    
    # 2. Incidents
    c.execute('''CREATE TABLE IF NOT EXISTS incidents (id INTEGER PRIMARY KEY AUTOINCREMENT, inc_number TEXT, title TEXT, description TEXT, status TEXT, priority TEXT, notes TEXT, cah_manager TEXT, assigned_bts_member TEXT, affected_user TEXT, ssd_it_assigned_to TEXT, source_category TEXT, specific_source TEXT, issue_type TEXT, sn_comments TEXT, bts_notes TEXT, mrn TEXT, workaround TEXT, resolution TEXT, date_ticket_created DATE, date_received_bts DATE, date_escalated_dt DATE, date_reported_epic DATE, project_id INTEGER, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')

    # 3. Projects
    c.execute('''CREATE TABLE IF NOT EXISTS projects (id INTEGER PRIMARY KEY AUTOINCREMENT, project_name TEXT NOT NULL, project_code TEXT UNIQUE, description TEXT, project_manager TEXT, business_owner TEXT, executive_sponsor TEXT, assigned_members TEXT, status TEXT DEFAULT 'Planning', start_date DATE, target_end_date DATE, actual_end_date DATE, budget_hours REAL, priority TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')

    # 4. Time logs
    c.execute('''CREATE TABLE IF NOT EXISTS time_logs (id INTEGER PRIMARY KEY AUTOINCREMENT, project_id INTEGER NOT NULL, user_id INTEGER NOT NULL, date DATE NOT NULL, hours REAL NOT NULL, description TEXT, category TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE, FOREIGN KEY (user_id) REFERENCES users(id))''')

    # 5. Project updates (History)
    c.execute('''CREATE TABLE IF NOT EXISTS project_updates (id INTEGER PRIMARY KEY AUTOINCREMENT, project_id INTEGER NOT NULL, update_type TEXT NOT NULL, user_name TEXT, update_text TEXT, old_value TEXT, new_value TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE)''')

    # 6. Milestones (NEW)
    c.execute('''CREATE TABLE IF NOT EXISTS project_milestones (id INTEGER PRIMARY KEY AUTOINCREMENT, project_id INTEGER, group_name TEXT, milestone_name TEXT, percent_complete INTEGER DEFAULT 0, start_date DATE, end_date DATE, comments TEXT, status TEXT, FOREIGN KEY(project_id) REFERENCES projects(id) ON DELETE CASCADE)''')

    # 7. Status Reports (NEW)
    c.execute('''CREATE TABLE IF NOT EXISTS status_reports (id INTEGER PRIMARY KEY AUTOINCREMENT, project_id INTEGER, report_date DATE, next_report_date DATE, health_scope TEXT, health_schedule TEXT, health_budget TEXT, health_resources TEXT, health_quality TEXT, health_overall TEXT, executive_summary TEXT, accomplishments TEXT, next_steps TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, FOREIGN KEY(project_id) REFERENCES projects(id) ON DELETE CASCADE)''')

    # Defaults
    c.execute("SELECT COUNT(*) as count FROM users")
    if c.fetchone()['count'] == 0:
        for member in DEFAULT_BTS_MEMBERS:
            try: 
                c.execute("INSERT INTO users (name, team, is_active) VALUES (?, ?, ?)", (member, 'BTS', 1))
            except Exception as e: print(f"Error init users: {e}")

    # Sample Projects
    c.execute("SELECT COUNT(*) as count FROM projects")
    if c.fetchone()['count'] == 0:
        samples = [
            ("AOP-25-0101", "Active Episodes – No SOC Workflow Optimization"),
            ("AOP-25-0102", "Supply Checkout Process Standardization"),
            ("BTS-25-0201", "eSmart File Manager"),
            ("CLX-25-0701", "Care Experience All Stars"),
            ("DME-25-0401", "DME CMS 2024 Alignment"),
            ("DME-25-0201", "RPA: DME Referral Touchpoints"),
            ("HHC-25-0101", "Kern Internalization"),
            ("HOS-25-0101", "After Hours Care Services Optimization"),
            ("MTS-25-0101", "Non-Scheduled 911 Activation Reduction"),
            ("PBI-25-0501", "Pre-Billing Enhancements 2025")
        ]
        
        mgr = DEFAULT_BTS_MEMBERS[0] 
        
        for code, name in samples:
            try: 
                c.execute('''INSERT INTO projects 
                    (project_name, project_code, description, project_manager, status, start_date, budget_hours, priority) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)''', 
                    (name, code, "2025 Strategic Initiative", mgr, "Active", "2025-01-01", 100.0, "High"))
            except Exception as e: print(f"Error init projects: {e}")

    # --- POPULATE RICH TEST DATA FOR "HOS-25-0101" ---
    # Find the After Hours project ID
    c.execute("SELECT id FROM projects WHERE project_code = 'HOS-25-0101'")
    res = c.fetchone()
    if res:
        pid = res['id']
        c.execute("SELECT count(*) as count FROM project_milestones WHERE project_id = ?", (pid,))
        if c.fetchone()['count'] == 0:
            milestones = [
                (pid, "AHCS Re-alignment", "Collect & analyze data to identify gaps", 80, "2024-11-01", None, "Reviewed productivity data, call volume...", "On Track"),
                (pid, "AHCS Re-alignment", "Evaluate Current Staffing Model", 90, "2024-11-01", None, "Evaluate staffing model per shift...", "On Track"),
                (pid, "KPATHS", "Develop Clinical Protocols", 100, "2024-09-01", "2024-11-30", "Completed. Dr. Rosen & Dr. Wong approved.", "Completed"),
                (pid, "KPATHS", "Develop Training Plan", 100, "2024-11-30", "2025-02-16", "Completed.", "Completed"),
                (pid, "KPATHS", "System & Access", 100, "2024-08-01", "2025-02-16", "Training & In Production environment complete.", "Completed"),
                (pid, "KPATHS", "Testing Phase", 100, "2025-03-03", "2025-03-31", "Validation occurred on 02/28.", "Completed"),
                (pid, "KPATHS", "Maintenance/Enhancements", 60, "2025-03-17", None, "Gathering suggested enhancements.", "On Track"),
                (pid, "24/7 Hour Model", "Data Collection & Needs Assessment", 100, "2025-01-27", "2025-09-25", "Completed data collection.", "Completed"),
                (pid, "24/7 Hour Model", "Framework Development", 80, "2025-02-03", "2025-11-30", "Starting to map out staffing model.", "On Track")
            ]
            c.executemany("INSERT INTO project_milestones (project_id, group_name, milestone_name, percent_complete, start_date, end_date, comments, status) VALUES (?,?,?,?,?,?,?,?)", milestones)
            
            c.execute('''INSERT INTO status_reports 
                (project_id, report_date, next_report_date, health_scope, health_schedule, health_budget, health_resources, health_quality, health_overall, executive_summary, accomplishments, next_steps)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                (pid, datetime.now().strftime('%Y-%m-%d'), (datetime.now() + timedelta(days=14)).strftime('%Y-%m-%d'),
                 "On Track", "On Track", "On Track", "On Track", "On Track", "On Track",
                 "The team is continuing to work on defining the implementation timeline and outline how the new team will be operationalized moving forward. Working on finalizing staffing model & timeline for future After Hours Care Services model with a focus on phase 1.",
                 "• Re-identified the AHCS Operational Goals to align with KPATHS.\n• Completed data collection for needs assessment.",
                 "• Re-communication & Accountability plan for in basket - pending\n• Finalize the HI AHCS Support Model\n• Update the framework of future model based on business case results")
            )

    # Migrations
    c.execute("PRAGMA table_info(incidents)")
    if 'project_id' not in [r['name'] for r in c.fetchall()]:
        try: c.execute("ALTER TABLE incidents ADD COLUMN project_id INTEGER")
        except Exception as e: print(f"Error migration: {e}")

    # NEW: Migrate Projects table to have business_owner and executive_sponsor
    c.execute("PRAGMA table_info(projects)")
    p_cols = [r['name'] for r in c.fetchall()]
    if 'business_owner' not in p_cols:
        try: c.execute("ALTER TABLE projects ADD COLUMN business_owner TEXT")
        except Exception as e: print(f"Error adding business_owner: {e}")
    if 'executive_sponsor' not in p_cols:
        try: c.execute("ALTER TABLE projects ADD COLUMN executive_sponsor TEXT")
        except Exception as e: print(f"Error adding executive_sponsor: {e}")

    conn.commit()
    conn.close()

# --- Helper Functions ---
def safe_date(val):
    if pd.isna(val) or val == "" or val is None: return None
    try: return datetime.strptime(str(val).split()[0], '%Y-%m-%d')
    except: return None

def generate_next_project_code(team_code, type_code, year):
    conn = get_db_connection()
    yy = str(year)[-2:]
    prefix = f"{team_code}-{yy}-{type_code}"
    try:
        df = pd.read_sql_query("SELECT project_code FROM projects WHERE project_code LIKE ?", conn, params=(f"{prefix}%",))
        if df.empty: next_seq = 1
        else:
            sequences = []
            for code in df['project_code']:
                try:
                    if len(code) >= len(prefix) + 2: sequences.append(int(code[-2:]))
                except: pass
            next_seq = max(sequences) + 1 if sequences else 1
    except Exception as e:
        print(f"Error generating code: {e}")
        next_seq = 1
    finally: conn.close()
    return f"{prefix}{next_seq:02d}"

# --- Data Access ---
@st.cache_data(ttl=60) # Performance: Cache user list
def get_users(active_only=True, team=None):
    conn = get_db_connection()
    q = "SELECT * FROM users"
    conds, params = [], []
    if active_only: conds.append("is_active = 1")
    if team: 
        conds.append("team = ?")
        params.append(team)
    if conds: q += " WHERE " + " AND ".join(conds)
    q += " ORDER BY name"
    df = pd.read_sql_query(q, conn, params=params)
    conn.close()
    return df

def create_user(name, team):
    if not name or not team: return False
    conn = get_db_connection()
    try:
        conn.execute("INSERT INTO users (name, team, is_active) VALUES (?, ?, 1)", (name, team))
        conn.commit()
        get_users.clear() # Fix: Invalidate cache
        return True
    except Exception as e:
        print(f"Error creating user: {e}")
        return False
    finally: conn.close()

def update_user(user_id, name, team, is_active):
    conn = get_db_connection()
    try:
        conn.execute("UPDATE users SET name=?, team=?, is_active=? WHERE id=?", (name, team, is_active, user_id))
        conn.commit()
        get_users.clear() # Fix: Invalidate cache
        return True
    except Exception as e:
        print(f"Error updating user: {e}")
        return False
    finally: conn.close()

def delete_user(user_id):
    conn = get_db_connection()
    conn.execute("DELETE FROM users WHERE id=?", (user_id,))
    conn.commit()
    get_users.clear() # Fix: Invalidate cache
    conn.close()

@st.cache_data(ttl=60) # Performance: Cache project list
def get_projects():
    conn = get_db_connection()
    df = pd.read_sql_query("SELECT * FROM projects ORDER BY created_at DESC", conn)
    conn.close()
    if not df.empty:
        df['assigned_members'] = df['assigned_members'].apply(lambda x: json.loads(x) if x else [])
    return df

def get_project(project_id):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("SELECT * FROM projects WHERE id=?", (project_id,))
    res = c.fetchone()
    conn.close()
    if res:
        d = dict(res)
        d['assigned_members'] = json.loads(d.get('assigned_members') or '[]')
        return d
    return None

def create_project(data):
    if not data['project_name'] or not data['project_code']: return False
    conn = get_db_connection()
    c = conn.cursor()
    aj = json.dumps(data.get('assigned_members', []))
    c.execute('''INSERT INTO projects (project_name, project_code, description, project_manager, business_owner, executive_sponsor, assigned_members, status, start_date, target_end_date, budget_hours, priority) 
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
              (data['project_name'], data['project_code'], data['description'], data['project_manager'], data['business_owner'], data['executive_sponsor'], aj, data['status'], data['start_date'], data['target_end_date'], data['budget_hours'], data['priority']))
    pid = c.lastrowid
    log_project_update(c, pid, "Created", data.get('project_manager', 'System'), f"Project created: {data['project_name']}")
    conn.commit()
    get_projects.clear() # Fix: Invalidate cache
    conn.close()
    return pid

def update_project(project_id, data, user_name="System"):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("SELECT status FROM projects WHERE id=?", (project_id,))
    curr = c.fetchone()
    if curr and curr['status'] != data['status']:
        log_project_update(c, project_id, "Status Change", user_name, f"Status: {curr['status']} -> {data['status']}")
    
    aj = json.dumps(data.get('assigned_members', []))
    c.execute('''UPDATE projects SET project_name=?, description=?, project_manager=?, business_owner=?, executive_sponsor=?, assigned_members=?, status=?, start_date=?, target_end_date=?, actual_end_date=?, budget_hours=?, priority=?, updated_at=CURRENT_TIMESTAMP WHERE id=?''',
              (data['project_name'], data['description'], data['project_manager'], data['business_owner'], data['executive_sponsor'], aj, data['status'], data['start_date'], data['target_end_date'], data.get('actual_end_date'), data['budget_hours'], data['priority'], project_id))
    conn.commit()
    get_projects.clear() # Fix: Invalidate cache
    conn.close()

def delete_project(project_id):
    conn = get_db_connection()
    conn.execute("DELETE FROM projects WHERE id=?", (project_id,))
    conn.commit()
    get_projects.clear() # Fix: Invalidate cache
    conn.close()

def upsert_project_import(data):
    conn = get_db_connection()
    c = conn.cursor()
    # Check if project exists by code
    c.execute("SELECT id, status FROM projects WHERE project_code = ?", (data['project_code'],))
    res = c.fetchone()
    conn.close()
    
    # Clean list data (assigned_members from string "A, B" to list)
    if isinstance(data.get('assigned_members'), str):
        data['assigned_members'] = [x.strip() for x in data['assigned_members'].split(',') if x.strip()]
    
    # Clean dates
    for d in ['start_date', 'target_end_date', 'actual_end_date']:
        if pd.isna(data.get(d)) or data.get(d) == "": data[d] = None
        
    if res:
        # Update
        update_project(res['id'], data, "Bulk Import")
    else:
        # Create
        create_project(data)

# --- Milestones & Reports ---

def get_milestones(pid):
    conn = get_db_connection()
    df = pd.read_sql_query("SELECT * FROM project_milestones WHERE project_id=? ORDER BY start_date", conn, params=(pid,))
    conn.close()
    return df

def upsert_milestone(data):
    conn = get_db_connection()
    if data.get('id'):
        conn.execute("UPDATE project_milestones SET group_name=?, milestone_name=?, percent_complete=?, start_date=?, end_date=?, comments=?, status=? WHERE id=?",
                     (data['group_name'], data['milestone_name'], data['percent_complete'], data['start_date'], data['end_date'], data['comments'], data['status'], data['id']))
    else:
        conn.execute("INSERT INTO project_milestones (project_id, group_name, milestone_name, percent_complete, start_date, end_date, comments, status) VALUES (?,?,?,?,?,?,?,?)",
                     (data['project_id'], data['group_name'], data['milestone_name'], data['percent_complete'], data['start_date'], data['end_date'], data['comments'], data['status']))
    conn.commit()
    conn.close()

def delete_milestone(mid):
    conn = get_db_connection()
    conn.execute("DELETE FROM project_milestones WHERE id=?", (mid,))
    conn.commit()
    conn.close()

def get_latest_status_report(pid):
    conn = get_db_connection()
    df = pd.read_sql_query("SELECT * FROM status_reports WHERE project_id=? ORDER BY report_date DESC LIMIT 1", conn, params=(pid,))
    conn.close()
    return df.iloc[0] if not df.empty else None

def create_status_report(data):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute('''INSERT INTO status_reports (project_id, report_date, next_report_date, health_scope, health_schedule, health_budget, health_resources, health_quality, health_overall, executive_summary, accomplishments, next_steps)
                 VALUES (?,?,?,?,?,?,?,?,?,?,?,?)''',
              (data['project_id'], data['report_date'], data['next_report_date'], data['health_scope'], data['health_schedule'], data['health_budget'], 
               data['health_resources'], data['health_quality'], data['health_overall'], data['executive_summary'], data['accomplishments'], data['next_steps']))
    log_project_update(c, data['project_id'], "Status Report", "System", "New formal status report published")
    conn.commit()
    conn.close()

# --- Common Access ---
def log_project_update(cursor, pid, utype, user, text):
    cursor.execute("INSERT INTO project_updates (project_id, update_type, user_name, update_text) VALUES (?,?,?,?)", (pid, utype, user, text))

def add_status_update(pid, user, text):
    conn = get_db_connection()
    c = conn.cursor()
    log_project_update(c, pid, "Status Update", user, text)
    conn.commit()
    conn.close()

def get_project_history(pid):
    conn = get_db_connection()
    df = pd.read_sql_query("SELECT * FROM project_updates WHERE project_id=? ORDER BY created_at DESC", conn, params=(pid,))
    conn.close()
    return df

def log_time_entry(data):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("INSERT INTO time_logs (project_id, user_id, date, hours, description, category) VALUES (?,?,?,?,?,?)",
              (data['project_id'], data['user_id'], data['date'], data['hours'], data['description'], data['category']))
    c.execute("SELECT name FROM users WHERE id=?", (data['user_id'],))
    res = c.fetchone()
    uname = res['name'] if res else "Unknown"
    log_project_update(c, data['project_id'], "Time Logged", uname, f"{data['hours']}h logged: {data['description']}")
    conn.commit()
    conn.close()

def get_time_logs(pid=None):
    conn = get_db_connection()
    q = "SELECT t.id, t.date, t.hours, t.description, t.category, u.name as user_name, p.project_name, p.project_code, p.budget_hours FROM time_logs t JOIN users u ON t.user_id = u.id JOIN projects p ON t.project_id = p.id"
    p = []
    if pid:
        q += " WHERE t.project_id = ?"
        p.append(pid)
    q += " ORDER BY t.date DESC"
    df = pd.read_sql_query(q, conn, params=p if p else None)
    conn.close()
    return df

def upsert_incident(data, id=None):
    if not data.get('inc_number'): return 
    conn = get_db_connection()
    c = conn.cursor()
    for d in ['date_ticket_created', 'date_received_bts', 'date_escalated_dt', 'date_reported_epic']:
        if data.get(d) == "": data[d] = None
    if id:
        set_c = ', '.join([f"{k}=?" for k in data.keys()])
        c.execute(f"UPDATE incidents SET {set_c} WHERE id=?", list(data.values()) + [id])
    else:
        cols = ', '.join(data.keys())
        phs = ', '.join(['?']*len(data))
        c.execute(f"INSERT INTO incidents ({cols}) VALUES ({phs})", list(data.values()))
    conn.commit()
    conn.close()

def get_incidents():
    conn = get_db_connection()
    df = pd.read_sql_query("SELECT * FROM incidents ORDER BY created_at DESC", conn)
    conn.close()
    return df

def delete_records(table, ids):
    if not ids: return
    conn = get_db_connection()
    phs = ','.join(['?']*len(ids))
    conn.execute(f"DELETE FROM {table} WHERE id IN ({phs})", ids)
    conn.commit()
    conn.close()

def update_bulk_incidents(ids, updates):
    if not ids or not updates: return
    conn = get_db_connection()
    parts = [f"{k}=?" for k in updates.keys()]
    conn.execute(f"UPDATE incidents SET {', '.join(parts)} WHERE id IN ({','.join(['?']*len(ids))})", list(updates.values()) + ids)
    conn.commit()
    conn.close()

# --- VISUALIZERS ---
def render_status_card(proj, latest, milestones):
    with st.container(border=True):
        st.subheader(f"{proj['project_name']} ({proj['project_code']})")
        # Header Stats
        h1, h2, h3, h4, h5, h6 = st.columns(6)
        def render_health(label, val):
            icon = HEALTH_COLORS.get(val, "⚪")
            st.markdown(f"**{label}**<br>{icon} {val}", unsafe_allow_html=True)
        
        with h1: render_health("Scope", latest['health_scope'])
        with h2: render_health("Schedule", latest['health_schedule'])
        with h3: render_health("Budget", latest['health_budget'])
        with h4: render_health("Resources", latest['health_resources'])
        with h5: render_health("Quality", latest['health_quality'])
        with h6: render_health("OVERALL", latest['health_overall'])
        
        st.markdown("---")
        
        # Roles
        c_roles1, c_roles2, c_roles3 = st.columns(3)
        with c_roles1: st.markdown(f"**Project Manager:** {proj['project_manager'] or '-'}")
        with c_roles2: st.markdown(f"**Business Process Owner:** {proj['business_owner'] or '-'}")
        with c_roles3: st.markdown(f"**Executive Sponsor:** {proj['executive_sponsor'] or '-'}")

        st.markdown("---")

        # Split View
        col_left, col_right = st.columns([2, 1])
        
        with col_left:
            st.markdown("#### Schedule")
            if not milestones.empty:
                disp_ms = milestones[['milestone_name', 'percent_complete', 'start_date', 'end_date', 'comments']].copy()
                st.dataframe(disp_ms, hide_index=True, use_container_width=True)
            else:
                st.caption("No milestones defined.")
                
        with col_right:
            st.markdown("#### Executive Summary")
            st.info(latest['executive_summary'] or "No summary.")
            
            st.markdown("#### Accomplishments")
            st.write(latest['accomplishments'] or "-")
            
            st.markdown("#### Next Steps")
            st.write(latest['next_steps'] or "-")
            
        st.caption(f"Report Date: {latest['report_date']} | Next Report: {latest['next_report_date']}")

def render_project_overview_table(active_projs):
    """Renders the tabular view of all active projects for the Overview tab."""
    if active_projs.empty:
        st.info("No active projects to display.")
        return

    overview_data = []
    
    for _, proj in active_projs.iterrows():
        latest_rep = get_latest_status_report(proj['id'])
        
        status_val = latest_rep['health_overall'] if latest_rep is not None else "Not Started"
        status_icon = HEALTH_COLORS.get(status_val, "⚪")
        
        frequency = "Biweekly" 
        
        team_code = proj['project_code'].split('-')[0] if '-' in proj['project_code'] else "UNK"
        team_name = TEAMS.get(team_code, team_code)
        
        row = {
            "Alert": status_icon,
            "Project Name": proj['project_name'],
            "Project Lead": proj['project_manager'],
            "Project Team": team_name, 
            "Status": status_val,
            "Frequency": frequency,
            "Project ETC": proj['target_end_date']
        }
        overview_data.append(row)
        
    df_overview = pd.DataFrame(overview_data)
    
    st.dataframe(
        df_overview,
        column_config={
            "Alert": st.column_config.TextColumn("Alert", width="small"),
            "Project Name": st.column_config.TextColumn("Project Name", width="large"),
            "Status": st.column_config.TextColumn("Status", width="medium"),
        },
        hide_index=True,
        use_container_width=True
    )


# --- FORMS ---
def incident_form(key_prefix, d=None):
    d = d or {}
    projs = get_projects()
    p_opts = ["None"] + [f"{r['project_code']} - {r['project_name']}" for _, r in projs.iterrows()]
    bts = get_users(active_only=True, team='BTS')['name'].tolist()
    
    st.markdown("### 🎫 Ticket ID")
    c1,c2,c3,c4 = st.columns(4)
    inc = c1.text_input("INC#", d.get('inc_number',''), placeholder="ID/NA", key=f"{key_prefix}_i")
    mrn = c2.text_input("MRN", d.get('mrn',''), key=f"{key_prefix}_m")
    iss = c3.selectbox("Type", [""]+ISSUE_TYPES, index=ISSUE_TYPES.index(d.get('issue_type'))+1 if d.get('issue_type') in ISSUE_TYPES else 0, key=f"{key_prefix}_it")
    stat = c4.selectbox("Status", STATUS_OPTIONS, index=STATUS_OPTIONS.index(d.get('status')) if d.get('status') in STATUS_OPTIONS else 0, key=f"{key_prefix}_st")
    
    tit = st.text_input("Summary", d.get('title',''), key=f"{key_prefix}_ti")
    
    curr_p = d.get('project_id')
    pidx = 0
    if curr_p and not projs.empty:
        match = projs[projs['id']==curr_p]
        if not match.empty:
            pstr = f"{match.iloc[0]['project_code']} - {match.iloc[0]['project_name']}"
            if pstr in p_opts: pidx = p_opts.index(pstr)
    lp = st.selectbox("Link Project", p_opts, index=pidx, key=f"{key_prefix}_lp")
    
    st.markdown("### 👥 Assign")
    p1,p2,p3,p4 = st.columns(4)
    mgr = p1.text_input("Mgr", d.get('cah_manager',''), key=f"{key_prefix}_mg")
    cur = d.get('assigned_bts_member','')
    idx = (["Unassigned"]+bts).index(cur) if cur in (["Unassigned"]+bts) else 0
    abts = p2.selectbox("BTS", ["Unassigned"]+bts, index=idx, key=f"{key_prefix}_bt")
    aff = p3.text_input("Aff. User", d.get('affected_user',''), key=f"{key_prefix}_af")
    ssd = p4.text_input("SSD/IT", d.get('ssd_it_assigned_to',''), key=f"{key_prefix}_sd")
    
    st.markdown("### 📅 Dates")
    d1,d2,d3,d4 = st.columns(4)
    dt1 = d1.date_input("Created", safe_date(d.get('date_ticket_created')), key=f"{key_prefix}_d1")
    dt2 = d2.date_input("Rec'd", safe_date(d.get('date_received_bts')), key=f"{key_prefix}_d2")
    dt3 = d3.date_input("Esc D/T", safe_date(d.get('date_escalated_dt')), key=f"{key_prefix}_d3")
    dt4 = d4.date_input("Epic", safe_date(d.get('date_reported_epic')), key=f"{key_prefix}_d4")
    
    st.markdown("### 📂 Details")
    x1,x2,x3 = st.columns([1,2,1])
    src = x1.selectbox("Source", [""]+SOURCE_CATEGORIES, index=SOURCE_CATEGORIES.index(d.get('source_category'))+1 if d.get('source_category') in SOURCE_CATEGORIES else 0, key=f"{key_prefix}_src")
    spec = x2.text_input("Spec Source", d.get('specific_source',''), key=f"{key_prefix}_sp")
    wa = x3.radio("WA?", WORKAROUND_OPTIONS, index=WORKAROUND_OPTIONS.index(d.get('workaround')) if d.get('workaround') in WORKAROUND_OPTIONS else 1, horizontal=True, key=f"{key_prefix}_wa")
    
    n1,n2 = st.columns(2)
    snc = n1.text_area("SN Comments", d.get('sn_comments',''), height=100, key=f"{key_prefix}_sc")
    btsn = n1.text_area("BTS Notes", d.get('bts_notes',''), height=100, key=f"{key_prefix}_bn")
    res = n2.text_area("Resolution", d.get('resolution',''), height=240, key=f"{key_prefix}_res")
    
    pid = None
    if lp != "None" and not projs.empty:
        code = lp.split(" - ")[0]
        m = projs[projs['project_code']==code]
        if not m.empty: pid = int(m.iloc[0]['id'])
        
    return {
        'inc_number': inc, 'title': tit, 'status': stat, 'mrn': mrn, 'issue_type': iss, 
        'cah_manager': mgr, 'assigned_bts_member': abts if abts != "Unassigned" else "", 
        'affected_user': aff, 'ssd_it_assigned_to': ssd,
        'date_ticket_created': dt1, 'date_received_bts': dt2, 'date_escalated_dt': dt3, 'date_reported_epic': dt4,
        'source_category': src, 'specific_source': spec, 'workaround': wa,
        'sn_comments': snc, 'bts_notes': bts_nt, 'resolution': res, 'project_id': pid
    }

def project_form(key_prefix, d=None):
    d = d or {}
    users = get_users(active_only=True)['name'].tolist()
    is_new = d == {}
    
    c1, c2 = st.columns(2)
    with c1:
        nm = st.text_input("Name *", d.get('project_name', ''), key=f"{key_prefix}_nm")
        if is_new:
            st.caption("Code Generator")
            gc1, gc2, gc3 = st.columns([1,1,1])
            t = gc1.selectbox("Team", list(TEAMS.keys()), key=f"{key_prefix}_t")
            tp = gc2.selectbox("Type", list(PROJECT_TYPES.keys()), format_func=lambda x: f"{x}-{PROJECT_TYPES[x]}", key=f"{key_prefix}_tp")
            yr = gc3.number_input("Year", 2024, 2030, datetime.now().year, key=f"{key_prefix}_yr")
            cd = generate_next_project_code(t, tp, yr)
            st.info(f"Code: {cd}")
        else:
            cd = st.text_input("Code", d.get('project_code',''), disabled=True, key=f"{key_prefix}_cd")
        
        cidx = users.index(d.get('project_manager'))+1 if d.get('project_manager') in users else 0
        pm = st.selectbox("Project Manager *", [""]+users, index=cidx, key=f"{key_prefix}_pm")
    
    with c2:
        stt = st.selectbox("Status", PROJECT_STATUS_OPTIONS, index=PROJECT_STATUS_OPTIONS.index(d.get('status','Planning')), key=f"{key_prefix}_st")
        pri = st.selectbox("Priority", ["Low","Medium","High","Critical"], index=["Low","Medium","High","Critical"].index(d.get('priority','Medium')), key=f"{key_prefix}_pr")
        bg = st.number_input("Budget (H)", 0.0, step=0.5, value=float(d.get('budget_hours',0)), key=f"{key_prefix}_bg")
        
        bpo_idx = users.index(d.get('business_owner'))+1 if d.get('business_owner') in users else 0
        bpo = st.selectbox("Business Process Owner", [""]+users, index=bpo_idx, key=f"{key_prefix}_bpo")
        
        esp_idx = users.index(d.get('executive_sponsor'))+1 if d.get('executive_sponsor') in users else 0
        esp = st.selectbox("Executive Sponsor", [""]+users, index=esp_idx, key=f"{key_prefix}_esp")
    
    dsc = st.text_area("Description", d.get('description',''), key=f"{key_prefix}_ds")
    mem = st.multiselect("Team", users, default=d.get('assigned_members', []), key=f"{key_prefix}_mem")
    
    d1, d2, d3 = st.columns(3)
    sd = d1.date_input("Start", safe_date(d.get('start_date')), key=f"{key_prefix}_sd")
    td = d2.date_input("Target", safe_date(d.get('target_end_date')), key=f"{key_prefix}_td")
    ad = d3.date_input("Actual", safe_date(d.get('actual_end_date')), key=f"{key_prefix}_ad")
    
    return {
        'project_name': nm, 'project_code': cd, 'description': dsc, 'project_manager': pm,
        'business_owner': bpo, 'executive_sponsor': esp,
        'assigned_members': mem, 'status': stt, 'start_date': sd, 'target_end_date': td, 
        'actual_end_date': ad, 'budget_hours': bg, 'priority': pri
    }

# --- LANDING ---
def landing_page():
    st.markdown("""
    <div style='text-align: center; padding: 3rem 1rem;'>
        <h1 style='font-size: 3.5rem; margin-bottom: 0.5rem;'>🌿 Athelas</h1>
        <h3 style='color: #4a4a4a; font-weight: 300; margin-bottom: 2rem;'>KP Care at Home Incident and Project Tracking Portal</h3>
        <div style='background-color: #f0f2f6; padding: 1.5rem; border-radius: 10px; max_width: 700px; margin: 0 auto; border-left: 5px solid #2e7bcf;'>
            <p style='font-size: 1.1rem; line-height: 1.6; color: #333; margin: 0;'>
                In <b>Lord of the Rings</b>, <b>Athelas</b> is a healing herb with potent restorative properties. Similarly, this tool is designed to bring order and resolution to our technical challenges. By tracking incidents and managing projects effectively, we ensure the health of our operations and support the vital care provided to our patients at home.
            </p>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    # User Selection Logic
    all_u = get_users(active_only=True)
    
    current_user_name = ""
    if st.session_state.curr_user_id:
        user_row = all_u[all_u['id'] == st.session_state.curr_user_id]
        if not user_row.empty:
            current_user_name = user_row.iloc[0]['name']

    with st.container():
        c_team, c_user = st.columns(2)
        with c_team:
            sel_team_display = st.selectbox("1. Filter by Team", ["All Teams"] + list(TEAMS.keys()), index=0)
        with c_user:
            if sel_team_display != "All Teams":
                filtered_users = all_u[all_u['team'] == sel_team_display]
            else:
                filtered_users = all_u
            
            user_map = {u['name']: u['id'] for _, u in filtered_users.iterrows()}
            curr_name_in_list = current_user_name if current_user_name in user_map else ""
            
            sel_user = st.selectbox(
                "2. Select User", 
                [""] + list(user_map.keys()), 
                index=list(user_map.keys()).index(curr_name_in_list)+1 if curr_name_in_list else 0
            )
            
            if sel_user:
                st.session_state.curr_user_id = user_map[sel_user]
            else:
                st.session_state.curr_user_id = None

    st.markdown("---")
    
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("### ⛑️ Incident Desk")
        st.caption("Fast ticketing & support resolution.")
        if st.button("Open Incidents", use_container_width=True, type="primary"): 
            st.session_state.page = "incidents"
            st.rerun()
            
    with c2:
        st.markdown("### ⏱️ Project Hub")
        st.caption("Project planning, milestones & time logs.")
        if st.button("Open Projects", use_container_width=True, type="primary"): 
            st.session_state.page = "projects"
            st.rerun()
            
    st.markdown("<br><br>", unsafe_allow_html=True)
    _, c_admin, _ = st.columns([2, 1, 2])
    with c_admin:
        if st.button("🔒 System Admin", use_container_width=True):
            st.session_state.page = "admin_auth"
            st.rerun()

def render_home_btn():
    if st.sidebar.button("🏠 Home", use_container_width=True): st.session_state.page = "home"; st.rerun()
    st.sidebar.markdown("---")

# --- ROUTES ---
def route_incidents():
    render_home_btn()
    st.sidebar.title("🔴 Incidents")
    menu = st.sidebar.radio("Menu", ["Dashboard", "Log New", "Manage"])
    
    if menu == "Dashboard":
        if st.session_state.get('dash_edit_id'):
            st.title("📝 Edit")
            st.button("← Back", on_click=lambda: st.session_state.update(dash_edit_id=None))
            row = get_incidents().loc[get_incidents()['id'] == st.session_state.dash_edit_id].iloc[0]
            with st.form("de"):
                nd = incident_form("de", row.to_dict())
                if st.form_submit_button("Update", type="primary"):
                    upsert_incident(nd, st.session_state.dash_edit_id)
                    st.success("Updated"); st.session_state.dash_edit_id = None; time.sleep(0.5); st.rerun()
        else:
            st.title("📊 Dashboard")
            df = get_incidents()
            c1,c2,c3 = st.columns(3)
            c1.metric("Total", len(df))
            c2.metric("Active", len(df[~df['status'].isin(['Resolved','Closed'])]))
            c3.metric("Unassigned", len(df[(df['assigned_bts_member'].isin(['Unassigned',None,''])) & (~df['status'].isin(['Resolved','Closed']))]))
            
            f1,f2 = st.columns(2)
            sf = f1.multiselect("Status", STATUS_OPTIONS, ["New", "In Progress", "On Hold"])
            mf = f2.multiselect("Assignee", ["Unassigned"]+get_users(active_only=True, team='BTS')['name'].tolist())
            
            fil = df.copy()
            fil['assigned_bts_member'] = fil['assigned_bts_member'].fillna('Unassigned').replace('', 'Unassigned')
            if sf: fil = fil[fil['status'].isin(sf)]
            if mf: fil = fil[fil['assigned_bts_member'].isin(mf)]
            
            sel = st.dataframe(
                fil[['inc_number','status','assigned_bts_member','title','date_ticket_created','id']], 
                column_config={"id":None}, 
                hide_index=True, 
                on_select="rerun", 
                selection_mode="single-row", 
                use_container_width=True
            )
            if sel.selection.rows:
                selected_id = fil.iloc[sel.selection.rows[0]]['id']
                st.session_state.dash_edit_id = selected_id
                st.rerun()

    elif menu == "Log New":
        st.title("📝 Log New")
        with st.form("ln"):
            d = incident_form("ln")
            if st.form_submit_button("Save", type="primary"):
                upsert_incident(d); st.success("Saved")
    elif menu == "Manage":
        st.title("🛠️ Manage")
        df = get_incidents()
        mode = st.radio("Mode", ["Single", "Bulk"], horizontal=True)
        if mode == "Single":
            s = st.text_input("Search")
            if s: df = df[df.astype(str).apply(lambda x: x.str.contains(s, case=False)).any(axis=1)]
            if not df.empty:
                iid = st.selectbox("Select", df['id'].tolist(), format_func=lambda x: f"{df[df['id']==x].iloc[0]['inc_number']} - {df[df['id']==x].iloc[0]['title']}")
                with st.form("se"):
                    nd = incident_form("se", df[df['id']==iid].iloc[0].to_dict())
                    if st.form_submit_button("Update", type="primary"): upsert_incident(nd, iid); st.success("Updated"); time.sleep(0.5); st.rerun()
                if st.button("Delete"): delete_records('incidents', [iid]); st.success("Deleted"); st.rerun()
        else:
            if "bs" not in st.session_state: st.session_state.bs = False
            if st.button("Select All"): st.session_state.bs = True; st.rerun()
            dfs = df.copy(); dfs.insert(0,"Select",st.session_state.bs)
            ed = st.data_editor(dfs, hide_index=True, column_config={"Select":st.column_config.CheckboxColumn(width="small")}, disabled=df.columns)
            sel = ed[ed.Select]
            if not sel.empty:
                if st.button("Delete Selected"): delete_records('incidents', sel['id'].tolist()); st.session_state.bs = False; st.rerun()
                with st.form("bulk"):
                    ns = st.selectbox("Status", ["(No Change)"]+STATUS_OPTIONS)
                    na = st.selectbox("Assignee", ["(No Change)","Unassigned"]+get_users(active_only=True, team='BTS')['name'].tolist())
                    if st.form_submit_button("Update"):
                        u = {}
                        if ns != "(No Change)": u['status'] = ns
                        if na != "(No Change)": u['assigned_bts_member'] = "" if na == "Unassigned" else na
                        if u: update_bulk_incidents(sel['id'].tolist(), u); st.session_state.bs = False; st.rerun()

def route_projects():
    render_home_btn()
    st.sidebar.title("🔵 Projects")
    menu = st.sidebar.radio("Menu", ["Analytics", "Manage Projects", "Status Reports", "Time Tracking"])
    
    if menu == "Analytics":
        st.title("📊 Analytics")
        logs = get_time_logs()
        projs = get_projects()
        
        c1,c2,c3 = st.columns(3)
        c1.metric("Logged Hours", f"{logs['hours'].sum():.1f}")
        c2.metric("Active Projects", len(projs[projs['status']=='Active']))
        c3.metric("Contributors", logs['user_name'].nunique() if not logs.empty else 0)
        st.markdown("---")
        
        if not logs.empty:
            c1,c2 = st.columns(2)
            c1.markdown("#### ⏳ By Project")
            c1.bar_chart(logs.groupby("project_name")['hours'].sum())
            c2.markdown("#### 🏆 By Person")
            c2.bar_chart(logs.groupby("user_name")['hours'].sum())
            
    elif menu == "Manage Projects":
        st.title("📁 Manage Projects")
        
        projs = get_projects()
        if projs.empty:
            st.info("No projects.")
            with st.expander("Create New Project", expanded=True):
                with st.form("np"):
                    pd_data = project_form("np")
                    if st.form_submit_button("Create", type="primary"):
                        if not pd_data['project_name']: st.error("Name required")
                        else: create_project(pd_data); st.success("Created"); st.rerun()
        else:
            c_sel, c_new = st.columns([3, 1])
            pid = c_sel.selectbox("Select Project", projs['id'].tolist(), format_func=lambda x: f"{projs[projs['id']==x].iloc[0]['project_code']} - {projs[projs['id']==x].iloc[0]['project_name']}", key="mp_selector")
            
            if c_new.button("➕ Create New"):
                st.session_state.creating_project = True
            
            if st.session_state.get('creating_project'):
                st.markdown("---")
                st.subheader("New Project")
                with st.form("np_overlay"):
                    pd_data = project_form("np_overlay")
                    c1, c2 = st.columns([1,5])
                    if c1.form_submit_button("Create"):
                        create_project(pd_data); st.session_state.creating_project = False; st.rerun()
                    if c2.form_submit_button("Cancel"):
                        st.session_state.creating_project = False; st.rerun()
                st.markdown("---")

            proj = get_project(pid)
            
            pt1, pt2, pt3, pt4 = st.tabs(["Details", "Schedule (Milestones)", "Status Reports", "History"])
            
            with pt1: 
                with st.form(f"ep_{pid}"):
                    upd = project_form(f"ep_{pid}", proj)
                    if st.form_submit_button("Update", type="primary"):
                        update_project(pid, upd, "System"); st.success("Updated"); st.rerun()
                if st.button("Delete Project"): delete_project(pid); st.success("Deleted"); st.rerun()

            with pt2: 
                st.markdown("### 📅 Project Schedule")
                milestones = get_milestones(pid)
                if not milestones.empty:
                    st.dataframe(milestones[['group_name', 'milestone_name', 'percent_complete', 'start_date', 'end_date', 'status', 'comments']], hide_index=True, use_container_width=True)
                
                with st.expander("➕ Add / Edit Milestone"):
                    ms_id = None
                    if not milestones.empty:
                        edit_ms = st.selectbox("Edit Existing?", ["(New Milestone)"] + milestones['milestone_name'].tolist(), key=f"ms_sel_{pid}")
                        if edit_ms != "(New Milestone)":
                            ms_row = milestones[milestones['milestone_name'] == edit_ms].iloc[0]
                            ms_id = ms_row['id']
                            d_ms = ms_row.to_dict()
                        else: d_ms = {}
                    else: d_ms = {}

                    with st.form(f"ms_form_{pid}"):
                        mc1, mc2 = st.columns(2)
                        grp = mc1.text_input("Group/Phase", d_ms.get('group_name', ''))
                        mnm = mc2.text_input("Milestone Name", d_ms.get('milestone_name', ''))
                        
                        mc3, mc4, mc5 = st.columns(3)
                        mpc = mc3.slider("% Complete", 0, 100, d_ms.get('percent_complete', 0))
                        msd = mc4.date_input("Start", safe_date(d_ms.get('start_date')))
                        med = mc5.date_input("End", safe_date(d_ms.get('end_date')))
                        
                        mc6, mc7 = st.columns(2)
                        mst = mc6.selectbox("Status", ["On Track", "At Risk", "Off Track", "Completed"], index=["On Track", "At Risk", "Off Track", "Completed"].index(d_ms.get('status', 'On Track')))
                        mcm = mc7.text_input("Comments", d_ms.get('comments', ''))
                        
                        if st.form_submit_button("Save Milestone"):
                            upsert_milestone({
                                'id': ms_id, 'project_id': pid, 'group_name': grp, 
                                'milestone_name': mnm, 'percent_complete': mpc, 
                                'start_date': msd, 'end_date': med, 'comments': mcm, 'status': mst
                            })
                            st.success("Saved"); st.rerun()
                    
                    if ms_id:
                        if st.button("Delete Milestone", key=f"del_ms_{ms_id}"):
                            delete_milestone(ms_id); st.success("Deleted"); st.rerun()

            with pt3: 
                st.markdown("### 📢 Status Reporting")
                latest = get_latest_status_report(pid)
                if latest is not None:
                    render_status_card(proj, latest, get_milestones(pid))
                
                st.markdown("---")
                with st.expander("➕ Create New Status Report"):
                    with st.form("new_stat_rep"):
                        rc1, rc2 = st.columns(2)
                        rdate = rc1.date_input("Report Date", datetime.now())
                        ndate = rc2.date_input("Next Report Out", datetime.now() + timedelta(days=14))
                        
                        st.markdown("**Health Indicators**")
                        hc1, hc2, hc3, hc4, hc5, hc6 = st.columns(6)
                        h_opts = ["On Track", "At Risk", "Off Track", "Not Started", "Completed"]
                        
                        h_sc = hc1.selectbox("Scope", h_opts)
                        h_sh = hc2.selectbox("Schedule", h_opts)
                        h_bu = hc3.selectbox("Budget", h_opts)
                        h_re = hc4.selectbox("Resources", h_opts)
                        h_qu = hc5.selectbox("Quality", h_opts)
                        h_ov = hc6.selectbox("OVERALL", h_opts)
                        
                        exec_sum = st.text_area("Executive Status Summary", height=100)
                        acc = st.text_area("Key Accomplishments", height=100)
                        nst = st.text_area("Next Steps", height=100)
                        
                        if st.form_submit_button("Publish Report"):
                            create_status_report({
                                'project_id': pid, 'report_date': rdate, 'next_report_date': ndate,
                                'health_scope': h_sc, 'health_schedule': h_sh, 'health_budget': h_bu,
                                'health_resources': h_re, 'health_quality': h_qu, 'health_overall': h_ov,
                                'executive_summary': exec_sum, 'accomplishments': acc, 'next_steps': nst
                            })
                            st.success("Published!"); st.rerun()

            with pt4: # History
                hist = get_project_history(pid)
                if not hist.empty:
                    st.dataframe(hist[['created_at','update_type','user_name','update_text']], hide_index=True, use_container_width=True)

    elif menu == "Status Reports":
        st.title("📊 Status Reporting")
        tabs = st.tabs(["Status Overview", "Executive Status Briefing"])
        
        with tabs[0]:
            st.markdown("### 📋 Project Status Update Overview")
            st.caption("High-level view of all active project health statuses and key information.")
            all_projs = get_projects()
            active_projs = all_projs[all_projs['status'] == 'Active']
            render_project_overview_table(active_projs)
        
        with tabs[1]:
            st.markdown("### 📄 Executive Status Briefing")
            st.caption("Detailed vertical rollup of latest status reports for all active projects.")
            
            if st.button("Generate Vertical Rollup"):
                st.markdown("## 📅 Executive Project Status Rollup")
                st.markdown(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M')}")
                st.markdown("---")
                
                all_projs = get_projects()
                active_projs = all_projs[all_projs['status'] == 'Active']
                
                if active_projs.empty:
                    st.warning("No active projects found.")
                else:
                    st.markdown("### 📋 High-Level Overview")
                    render_project_overview_table(active_projs)
                    st.markdown("---")
                    st.markdown("<br>", unsafe_allow_html=True)

                    for _, proj in active_projs.iterrows():
                        latest_rep = get_latest_status_report(proj['id'])
                        if latest_rep is not None:
                            ms = get_milestones(proj['id'])
                            render_status_card(proj, latest_rep, ms)
                            st.markdown("<br>", unsafe_allow_html=True) 
                        else:
                            pass
                st.success("End of Report")

    elif menu == "Time Tracking":
        st.title("⏱️ Log Time")
        if not st.session_state.curr_user_id: st.warning("Select User in sidebar"); return
        
        projs = get_projects()
        if projs.empty: st.info("No projects."); return
        
        c1, c2 = st.columns([2,1])
        with c1:
            with st.form("tl"):
                pid = st.selectbox("Project", projs['id'].tolist(), format_func=lambda x: projs[projs['id']==x].iloc[0]['project_name'])
                d1, d2 = st.columns(2)
                dt = d1.date_input("Date", datetime.now())
                hr = d2.number_input("Hours", 0.25, 24.0, 1.0, 0.25)
                cat = st.selectbox("Category", ["Dev", "Meeting", "Doc", "Support", "Other"])
                desc = st.text_area("Description")
                stat_up = st.text_area("Optional: Post as Status Update?")
                
                if st.form_submit_button("Log", type="primary"):
                    if not desc: st.error("Desc required")
                    else:
                        log_time_entry({'project_id':pid, 'user_id':st.session_state.curr_user_id, 'date':dt, 'hours':hr, 'description':desc, 'category':cat})
                        if stat_up:
                            un = get_users(active_only=False)
                            un = un[un['id']==st.session_state.curr_user_id].iloc[0]['name']
                            add_status_update(pid, un, stat_up)
                        st.success("Logged"); st.rerun()
        
        with c2:
            tlogs = get_time_logs()
            all_u = get_users(active_only=False)
            u_row = all_u[all_u['id'] == st.session_state.curr_user_id]
            if not u_row.empty:
                uname = u_row.iloc[0]['name']
                u_logs = tlogs[tlogs['user_name']==uname]
                st.metric("My Hours", u_logs['hours'].sum())
                st.dataframe(u_logs.head(5)[['date','project_name','hours']], hide_index=True)

def route_admin_auth():
    render_home_btn()
    st.title("🔐 Admin")
    pwd = st.text_input("Password", type="password")
    if st.button("Login"):
        if pwd == "CAH": st.session_state.page = "admin_panel"; st.rerun()
        else: st.error("Wrong Password")

def route_admin_panel():
    render_home_btn()
    st.sidebar.title("⚫ Admin")
    menu = st.sidebar.radio("Menu", ["Users", "Imports/Exports", "Logout"])
    
    if menu == "Users":
        st.title("👥 Users")
        t1,t2 = st.tabs(["Add","Manage"])
        with t1:
            with st.form("nu"):
                n = st.text_input("Name"); t = st.selectbox("Team", list(TEAMS.keys()))
                if st.form_submit_button("Add"): 
                    if create_user(n,t): st.success("Added")
                    else: st.error("Error")
        with t2:
            df = get_users(active_only=False)
            for _, u in df.iterrows():
                with st.expander(f"{u['name']} ({u['team']})"):
                    with st.form(f"eu_{u['id']}"):
                        nn = st.text_input("Name", u['name']); nt = st.selectbox("Team", list(TEAMS.keys()), index=list(TEAMS.keys()).index(u['team']) if u['team'] in TEAMS else 0); na = st.checkbox("Active", u['is_active'])
                        if st.form_submit_button("Update"): update_user(u['id'], nn, nt, na); st.success("Updated"); st.rerun()
                    if st.button("Delete", key=f"del_{u['id']}"): delete_user(u['id']); st.success("Deleted"); st.rerun()
    elif menu == "Imports/Exports":
        st.title("📤 Data Tools")
        
        st.subheader("Import Projects (CSV)")
        st.caption("Fields: project_name, project_code, status, project_manager, budget_hours, assigned_members")
        
        proj_template_cols = ['project_name', 'project_code', 'status', 'project_manager', 'budget_hours', 'priority', 'assigned_members', 'start_date', 'target_end_date', 'business_owner', 'executive_sponsor']
        proj_temp_df = pd.DataFrame(columns=proj_template_cols)
        st.download_button("Download Project Template", proj_temp_df.to_csv(index=False).encode('utf-8'), "project_import_template.csv", "text/csv")
        
        uploaded_proj = st.file_uploader("Upload Projects CSV", type="csv", key="proj_up")
        if uploaded_proj:
            try:
                try: df_p = pd.read_csv(uploaded_proj)
                except: 
                    uploaded_proj.seek(0)
                    df_p = pd.read_csv(uploaded_proj, encoding='cp1252')
                
                st.dataframe(df_p.head())
                if st.button("Confirm Project Import"):
                    count = 0
                    for _, r in df_p.iterrows(): 
                        upsert_project_import(r.to_dict())
                        count += 1
                    st.success(f"Imported {count} projects!")
            except Exception as e: st.error(str(e))

        st.markdown("---")
        st.subheader("Import Incidents (CSV)")
        
        inc_template_cols = ['inc_number', 'title', 'description', 'status', 'priority', 'notes', 'cah_manager', 'assigned_bts_member', 'affected_user', 'ssd_it_assigned_to', 'source_category', 'specific_source', 'issue_type', 'sn_comments', 'bts_notes', 'mrn', 'workaround', 'resolution', 'date_ticket_created', 'date_received_bts', 'date_escalated_dt', 'date_reported_epic']
        inc_temp_df = pd.DataFrame(columns=inc_template_cols)
        st.download_button("Download Incident Template", inc_temp_df.to_csv(index=False).encode('utf-8'), "incident_import_template.csv", "text/csv")

        uploaded_file = st.file_uploader("Upload Incidents", type="csv")
        if uploaded_file:
            try:
                try: df = pd.read_csv(uploaded_file)
                except: 
                    uploaded_file.seek(0)
                    df = pd.read_csv(uploaded_file, encoding='cp1252')
                if st.button("Confirm Incident Import"):
                    for _, r in df.iterrows(): upsert_incident(r.to_dict())
                    st.success("Done")
            except Exception as e: st.error(str(e))
            
        st.markdown("---")
        st.subheader("Export Database Tables")
        
        c1,c2,c3 = st.columns(3)
        with c1:
            inc_csv = get_incidents().to_csv(index=False).encode('utf-8')
            st.download_button("📥 Incidents", inc_csv, "incidents.csv", use_container_width=True)
            
            proj_df = get_projects()
            if not proj_df.empty:
                proj_df['assigned_members'] = proj_df['assigned_members'].apply(lambda x: ", ".join(x) if isinstance(x, list) else "")
            proj_csv = proj_df.to_csv(index=False).encode('utf-8')
            st.download_button("📥 Projects", proj_csv, "projects.csv", use_container_width=True)

        with c2:
            log_csv = get_time_logs().to_csv(index=False).encode('utf-8')
            st.download_button("📥 Time Logs", log_csv, "timelogs.csv", use_container_width=True)
            
            users_csv = get_users(active_only=False).to_csv(index=False).encode('utf-8')
            st.download_button("📥 Users", users_csv, "users.csv", use_container_width=True)

        with c3:
            conn = get_db_connection()
            hist_df = pd.read_sql_query("SELECT * FROM project_updates", conn)
            conn.close()
            hist_csv = hist_df.to_csv(index=False).encode('utf-8')
            st.download_button("📥 Project History", hist_csv, "project_history.csv", use_container_width=True)

    elif menu == "Logout": st.session_state.page = "home"; st.rerun()

# --- MAIN ---
def main():
    init_db()
    if 'page' not in st.session_state: st.session_state.page = "home"
    if 'curr_user_id' not in st.session_state: st.session_state.curr_user_id = None
    if 'dash_edit_id' not in st.session_state: st.session_state.dash_edit_id = None
    if 'inc_edit_id' not in st.session_state: st.session_state.inc_edit_id = None

    if st.session_state.page == "home": landing_page()
    elif st.session_state.page == "incidents": route_incidents()
    elif st.session_state.page == "projects": route_projects()
    elif st.session_state.page == "admin_auth": route_admin_auth()
    elif st.session_state.page == "admin_panel": route_admin_panel()

if __name__ == "__main__":
    main()