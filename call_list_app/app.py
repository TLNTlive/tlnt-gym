import sqlite3
import json
import os
from datetime import datetime, date, timedelta
from functools import wraps
from flask import (Flask, render_template, request, redirect, url_for,
                   flash, session, jsonify, g)

app = Flask(__name__)
app.secret_key = 'tlnt-gym-transition-2026'
DATABASE = os.environ.get('DATABASE_PATH', os.path.join(os.path.dirname(__file__), 'transition.db'))

TEAM_USERS = [
    'Lindsey Joyner',
    'Alan Belcher',
    'Monica Medina',
    'Nicholas Auxier',
    'Eli Stephens',
    'Lauren Majors',
]

WORKFLOW_STATUSES = [
    'Pending Outreach', 'Attempted', 'Contacted',
    'Critical Action', 'Completed', 'Not Interested', 'Do Not Migrate'
]

GYMDESK_STATUSES = ['Pending', 'Active', 'Inactive', 'Already Active in Gymdesk']
ABC_STATUSES = ['Active', 'Off', 'To Do']
ESCALATION_TAGS = ['Alan', 'Coach', 'Mike', 'Other']
CONTACT_TYPES = ['Call', 'Text', 'Email', 'In Person']
GYMDESK_OUTCOMES = ['Pending', 'Active', 'Inactive', 'Already Active in Gymdesk', 'Not Interested', 'Do Not Migrate']

# --------------- Database helpers ---------------

def get_db():
    if 'db' not in g:
        g.db = sqlite3.connect(DATABASE)
        g.db.row_factory = sqlite3.Row
        g.db.execute("PRAGMA journal_mode=WAL")
        g.db.execute("PRAGMA foreign_keys=ON")
    return g.db


@app.teardown_appcontext
def close_db(exception):
    db = g.pop('db', None)
    if db is not None:
        db.close()


def init_db():
    db = sqlite3.connect(DATABASE)
    db.executescript(SCHEMA)
    db.commit()
    db.close()


SCHEMA = """
CREATE TABLE IF NOT EXISTS members (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    member_number TEXT,
    agreement_number TEXT,
    member_name TEXT NOT NULL,
    first_name TEXT,
    last_name TEXT,
    address TEXT,
    city TEXT,
    state TEXT,
    zip TEXT,
    best_phone TEXT,
    email_address TEXT,
    date_of_birth TEXT,
    gender TEXT,
    membership_type TEXT,
    payment_plan TEXT,
    payment_mode TEXT,
    price REAL DEFAULT 0,
    price_bucket TEXT,
    payment_category TEXT,
    monthly_invoice REAL DEFAULT 0,
    start_date TEXT,
    last_billed_date TEXT,
    next_billing_date TEXT,
    next_due_date TEXT,
    conversion_priority TEXT,
    recommendation TEXT,
    -- Workflow fields
    workflow_status TEXT DEFAULT 'Pending Outreach',
    abc_status TEXT DEFAULT 'Active',
    gymdesk_status TEXT DEFAULT 'Pending',
    gymdesk_outcome TEXT DEFAULT 'Pending',
    gymdesk_checked INTEGER DEFAULT 0,
    abc_turned_off INTEGER DEFAULT 0,
    attempt_count INTEGER DEFAULT 0,
    last_contact_type TEXT DEFAULT '',
    last_contact_date TEXT DEFAULT '',
    last_contact_summary TEXT DEFAULT '',
    -- Escalation
    escalated INTEGER DEFAULT 0,
    escalation_tag TEXT DEFAULT '',
    escalation_note TEXT DEFAULT '',
    escalated_by TEXT DEFAULT '',
    escalated_to TEXT DEFAULT '',
    escalated_at TEXT DEFAULT '',
    -- Family
    family_group_id INTEGER,
    family_role TEXT DEFAULT 'Unknown',
    -- Notes
    notes TEXT DEFAULT '',
    -- Timestamps
    created_at TEXT,
    updated_at TEXT,
    FOREIGN KEY (family_group_id) REFERENCES family_groups(id)
);

CREATE TABLE IF NOT EXISTS family_groups (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    group_name TEXT NOT NULL,
    created_at TEXT
);

CREATE TABLE IF NOT EXISTS case_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    member_id INTEGER NOT NULL,
    timestamp TEXT NOT NULL,
    user_name TEXT NOT NULL,
    action_type TEXT NOT NULL,
    comment TEXT DEFAULT '',
    FOREIGN KEY (member_id) REFERENCES members(id)
);

CREATE INDEX IF NOT EXISTS idx_members_workflow ON members(workflow_status);
CREATE INDEX IF NOT EXISTS idx_members_abc ON members(abc_status);
CREATE INDEX IF NOT EXISTS idx_members_gymdesk ON members(gymdesk_status);
CREATE INDEX IF NOT EXISTS idx_members_escalated ON members(escalated);
CREATE INDEX IF NOT EXISTS idx_members_family ON members(family_group_id);
CREATE INDEX IF NOT EXISTS idx_history_member ON case_history(member_id);
"""


# --------------- Auth helper (simple session) ---------------

def current_user():
    return session.get('user_name', None)


def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not current_user():
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated


# --------------- Helper: add history ---------------

def add_history(db, member_id, action_type, comment=''):
    db.execute(
        "INSERT INTO case_history (member_id, timestamp, user_name, action_type, comment) VALUES (?,?,?,?,?)",
        (member_id, datetime.now().isoformat(), current_user(), action_type, comment)
    )


# --------------- Helper: check critical action ---------------

def check_critical_action(db, member_id):
    """If gymdesk outcome is resolved but ABC not off, set Critical Action."""
    m = db.execute("SELECT * FROM members WHERE id=?", (member_id,)).fetchone()
    if not m:
        return
    resolved_outcomes = ['Active', 'Inactive', 'Already Active in Gymdesk', 'Not Interested', 'Do Not Migrate']
    if m['gymdesk_outcome'] in resolved_outcomes and m['abc_status'] != 'Off':
        if m['workflow_status'] not in ('Completed',):
            db.execute("UPDATE members SET workflow_status='Critical Action', updated_at=? WHERE id=?",
                       (datetime.now().isoformat(), member_id))
    if m['abc_status'] == 'Off' and m['gymdesk_outcome'] in resolved_outcomes:
        db.execute("UPDATE members SET workflow_status='Completed', abc_turned_off=1, updated_at=? WHERE id=?",
                   (datetime.now().isoformat(), member_id))


# --------------- Routes: Auth ---------------

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        user_name = request.form.get('user_name')
        if user_name in TEAM_USERS:
            session['user_name'] = user_name
            return redirect(url_for('dashboard'))
        flash('Invalid user selection.', 'error')
    return render_template('login.html', users=TEAM_USERS)


@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))


# --------------- Routes: Dashboard ---------------

@app.route('/')
@login_required
def dashboard():
    db = get_db()
    counts = {}
    for status in WORKFLOW_STATUSES:
        counts[status] = db.execute("SELECT COUNT(*) FROM members WHERE workflow_status=?", (status,)).fetchone()[0]
    counts['Total Open'] = db.execute("SELECT COUNT(*) FROM members WHERE workflow_status NOT IN ('Completed')").fetchone()[0]
    counts['Escalated'] = db.execute("SELECT COUNT(*) FROM members WHERE escalated=1").fetchone()[0]
    counts['MIKE Escalations'] = db.execute("SELECT COUNT(*) FROM members WHERE escalated=1 AND escalation_tag='Mike'").fetchone()[0]
    counts['Gymdesk Active Complete'] = db.execute("SELECT COUNT(*) FROM members WHERE workflow_status='Completed' AND gymdesk_outcome='Active'").fetchone()[0]
    counts['Gymdesk Inactive Complete'] = db.execute("SELECT COUNT(*) FROM members WHERE workflow_status='Completed' AND gymdesk_outcome='Inactive'").fetchone()[0]
    counts['ABC Still Active'] = db.execute("SELECT COUNT(*) FROM members WHERE abc_status='Active'").fetchone()[0]
    counts['Not Interested Total'] = db.execute("SELECT COUNT(*) FROM members WHERE gymdesk_outcome='Not Interested' AND workflow_status != 'Completed'").fetchone()[0]
    counts['Do Not Migrate Total'] = db.execute("SELECT COUNT(*) FROM members WHERE gymdesk_outcome='Do Not Migrate' AND workflow_status != 'Completed'").fetchone()[0]

    today = date.today().isoformat()
    d7 = (date.today() + timedelta(days=7)).isoformat()
    d30 = (date.today() + timedelta(days=30)).isoformat()
    counts['Due Next 7 Days'] = db.execute("SELECT COUNT(*) FROM members WHERE next_billing_date != '' AND next_billing_date <= ? AND workflow_status NOT IN ('Completed')", (d7,)).fetchone()[0]
    counts['Due Next 30 Days'] = db.execute("SELECT COUNT(*) FROM members WHERE next_billing_date != '' AND next_billing_date <= ? AND workflow_status NOT IN ('Completed')", (d30,)).fetchone()[0]
    counts['Completed Today'] = db.execute("SELECT COUNT(*) FROM members WHERE workflow_status='Completed' AND updated_at LIKE ?", (today + '%',)).fetchone()[0]
    counts['Critical Actions Outstanding'] = counts.get('Critical Action', 0)

    return render_template('dashboard.html', counts=counts, user=current_user())


# --------------- Routes: Queue ---------------

@app.route('/queue')
@login_required
def queue():
    db = get_db()
    status_filter = request.args.get('status', '')
    search = request.args.get('search', '')
    priority_filter = request.args.get('priority', '')

    query = """SELECT m.*, fg.group_name as family_group_name,
               (SELECT COUNT(*) FROM members m2 WHERE m2.family_group_id = m.family_group_id AND m.family_group_id IS NOT NULL) as family_count
               FROM members m
               LEFT JOIN family_groups fg ON m.family_group_id = fg.id
               WHERE m.workflow_status NOT IN ('Completed')"""
    params = []

    if status_filter:
        query += " AND m.workflow_status = ?"
        params.append(status_filter)
    if priority_filter:
        query += " AND m.conversion_priority = ?"
        params.append(priority_filter)

    if search:
        query += " AND (m.member_name LIKE ? OR m.best_phone LIKE ? OR m.email_address LIKE ?)"
        params.extend([f'%{search}%'] * 3)

    today_day = date.today().day
    query += f""" ORDER BY
        CASE WHEN m.workflow_status = 'Critical Action' THEN 0 ELSE 1 END,
        CASE WHEN m.escalated = 1 AND m.escalation_tag = 'Mike' THEN 0
             WHEN m.escalated = 1 THEN 1 ELSE 2 END,
        CASE
            WHEN m.next_billing_date != '' AND LENGTH(m.next_billing_date) >= 10
            THEN (CAST(SUBSTR(m.next_billing_date, 9, 2) AS INTEGER) - {today_day} + 32) % 32
            WHEN m.next_due_date != '' AND LENGTH(m.next_due_date) >= 10
            THEN (CAST(SUBSTR(m.next_due_date, 9, 2) AS INTEGER) - {today_day} + 32) % 32
            ELSE 99
        END ASC,
        m.price DESC,
        m.last_contact_date ASC,
        m.family_group_id,
        m.id
    """

    members = db.execute(query, params).fetchall()
    today = date.today().isoformat()
    today_day = date.today().day
    return render_template('queue.html', members=members, user=current_user(),
                           status_filter=status_filter, search=search,
                           priority_filter=priority_filter,
                           workflow_statuses=WORKFLOW_STATUSES,
                           today=today, today_day=today_day)


# --------------- Routes: Escalated ---------------

@app.route('/escalated')
@login_required
def escalated_view():
    db = get_db()
    user = current_user()
    members = db.execute("""
        SELECT m.*, fg.group_name as family_group_name,
        (SELECT COUNT(*) FROM members m2 WHERE m2.family_group_id = m.family_group_id AND m.family_group_id IS NOT NULL) as family_count
        FROM members m
        LEFT JOIN family_groups fg ON m.family_group_id = fg.id
        WHERE m.escalated = 1
        ORDER BY
            CASE WHEN m.escalation_tag = 'Mike' THEN 0 ELSE 1 END,
            CASE WHEN m.escalated_to = ? THEN 0 ELSE 1 END,
            m.escalated_at DESC
    """, (user,)).fetchall()
    return render_template('escalated.html', members=members, user=user)


# --------------- Routes: Critical Action ---------------

@app.route('/critical')
@login_required
def critical_view():
    db = get_db()
    members = db.execute("""
        SELECT m.*, fg.group_name as family_group_name,
        (SELECT COUNT(*) FROM members m2 WHERE m2.family_group_id = m.family_group_id AND m.family_group_id IS NOT NULL) as family_count
        FROM members m
        LEFT JOIN family_groups fg ON m.family_group_id = fg.id
        WHERE m.workflow_status = 'Critical Action'
        ORDER BY m.next_billing_date ASC, m.price DESC
    """).fetchall()
    return render_template('critical.html', members=members, user=current_user())


# --------------- Routes: Completed ---------------

@app.route('/completed')
@login_required
def completed_view():
    db = get_db()
    members = db.execute("""
        SELECT m.*, fg.group_name as family_group_name
        FROM members m
        LEFT JOIN family_groups fg ON m.family_group_id = fg.id
        WHERE m.workflow_status = 'Completed'
        ORDER BY m.updated_at DESC
    """).fetchall()
    return render_template('completed.html', members=members, user=current_user())


# --------------- Routes: Not Interested / Do Not Migrate ---------------

@app.route('/not-interested')
@login_required
def not_interested_view():
    db = get_db()
    members = db.execute("""
        SELECT m.*, fg.group_name as family_group_name
        FROM members m
        LEFT JOIN family_groups fg ON m.family_group_id = fg.id
        WHERE m.gymdesk_outcome IN ('Not Interested', 'Do Not Migrate')
          AND m.workflow_status != 'Completed'
        ORDER BY m.abc_status ASC, m.updated_at DESC
    """).fetchall()
    return render_template('not_interested.html', members=members, user=current_user())


# --------------- Routes: Member Detail ---------------

@app.route('/member/<int:member_id>')
@login_required
def member_detail(member_id):
    db = get_db()
    member = db.execute("""
        SELECT m.*, fg.group_name as family_group_name
        FROM members m
        LEFT JOIN family_groups fg ON m.family_group_id = fg.id
        WHERE m.id = ?
    """, (member_id,)).fetchone()
    if not member:
        flash('Member not found.', 'error')
        return redirect(url_for('queue'))

    history = db.execute(
        "SELECT * FROM case_history WHERE member_id=? ORDER BY timestamp DESC", (member_id,)
    ).fetchall()

    family_members = []
    if member['family_group_id']:
        family_members = db.execute("""
            SELECT * FROM members WHERE family_group_id=? AND id != ? ORDER BY family_role DESC, member_name
        """, (member['family_group_id'], member_id)).fetchall()

    return render_template('member_detail.html', member=member, history=history,
                           family_members=family_members, user=current_user(),
                           workflow_statuses=WORKFLOW_STATUSES,
                           gymdesk_statuses=GYMDESK_STATUSES,
                           abc_statuses=ABC_STATUSES,
                           escalation_tags=ESCALATION_TAGS,
                           contact_types=CONTACT_TYPES,
                           gymdesk_outcomes=GYMDESK_OUTCOMES,
                           team_users=TEAM_USERS)


# --------------- Routes: Actions ---------------

@app.route('/member/<int:member_id>/contact', methods=['POST'])
@login_required
def log_contact(member_id):
    db = get_db()
    contact_type = request.form.get('contact_type')
    comment = request.form.get('comment', '').strip()
    now = datetime.now().isoformat()
    summary = f"[{contact_type}] {comment}" if comment else f"[{contact_type}]"

    db.execute("""UPDATE members SET
        attempt_count = attempt_count + 1,
        last_contact_type = ?,
        last_contact_date = ?,
        last_contact_summary = ?,
        workflow_status = CASE WHEN workflow_status = 'Pending Outreach' THEN 'Attempted' ELSE workflow_status END,
        updated_at = ?
        WHERE id = ?""",
        (contact_type, now, summary, now, member_id))
    add_history(db, member_id, f'Contact Attempt: {contact_type}', comment)
    db.commit()
    flash(f'{contact_type} logged.', 'success')
    return redirect(request.referrer or url_for('member_detail', member_id=member_id))


@app.route('/member/<int:member_id>/gymdesk-check', methods=['POST'])
@login_required
def gymdesk_check(member_id):
    db = get_db()
    now = datetime.now().isoformat()
    db.execute("UPDATE members SET gymdesk_checked=1, updated_at=? WHERE id=?", (now, member_id))
    add_history(db, member_id, 'Marked Gymdesk Checked')
    db.commit()
    flash('Gymdesk checked.', 'success')
    return redirect(url_for('member_detail', member_id=member_id))


@app.route('/member/<int:member_id>/set-gymdesk', methods=['POST'])
@login_required
def set_gymdesk(member_id):
    db = get_db()
    m = db.execute("SELECT gymdesk_checked FROM members WHERE id=?", (member_id,)).fetchone()
    if not m or not m['gymdesk_checked']:
        flash('You must check Gymdesk first before setting status.', 'error')
        return redirect(url_for('member_detail', member_id=member_id))

    status = request.form.get('gymdesk_status')
    now = datetime.now().isoformat()

    outcome_map = {
        'Active': 'Active',
        'Inactive': 'Inactive',
        'Already Active in Gymdesk': 'Already Active in Gymdesk',
    }
    outcome = outcome_map.get(status, 'Pending')

    db.execute("UPDATE members SET gymdesk_status=?, gymdesk_outcome=?, updated_at=? WHERE id=?",
               (status, outcome, now, member_id))
    add_history(db, member_id, f'Set Gymdesk {status}')
    check_critical_action(db, member_id)
    db.commit()
    flash(f'Gymdesk set to {status}. Please confirm ABC status.', 'warning')
    return redirect(url_for('member_detail', member_id=member_id))


@app.route('/member/<int:member_id>/set-abc', methods=['POST'])
@login_required
def set_abc(member_id):
    db = get_db()
    status = request.form.get('abc_status')
    now = datetime.now().isoformat()
    db.execute("UPDATE members SET abc_status=?, updated_at=? WHERE id=?", (status, now, member_id))
    if status == 'Off':
        db.execute("UPDATE members SET abc_turned_off=1, updated_at=? WHERE id=?", (now, member_id))
        add_history(db, member_id, 'Confirmed ABC Off')
    elif status == 'To Do':
        add_history(db, member_id, 'Added ABC to To Do')
    else:
        add_history(db, member_id, f'Set ABC {status}')
    check_critical_action(db, member_id)
    db.commit()
    flash(f'ABC set to {status}.', 'success')
    return redirect(request.referrer or url_for('member_detail', member_id=member_id))


@app.route('/member/<int:member_id>/mark-not-interested', methods=['POST'])
@login_required
def mark_not_interested(member_id):
    db = get_db()
    now = datetime.now().isoformat()
    db.execute("UPDATE members SET gymdesk_outcome='Not Interested', workflow_status='Not Interested', updated_at=? WHERE id=?",
               (now, member_id))
    add_history(db, member_id, 'Marked Not Interested')
    check_critical_action(db, member_id)
    db.commit()
    flash('Marked Not Interested.', 'info')
    return redirect(url_for('member_detail', member_id=member_id))


@app.route('/member/<int:member_id>/mark-do-not-migrate', methods=['POST'])
@login_required
def mark_do_not_migrate(member_id):
    db = get_db()
    now = datetime.now().isoformat()
    db.execute("UPDATE members SET gymdesk_outcome='Do Not Migrate', workflow_status='Do Not Migrate', updated_at=? WHERE id=?",
               (now, member_id))
    add_history(db, member_id, 'Marked Do Not Migrate')
    check_critical_action(db, member_id)
    db.commit()
    flash('Marked Do Not Migrate.', 'info')
    return redirect(url_for('member_detail', member_id=member_id))


@app.route('/member/<int:member_id>/escalate', methods=['POST'])
@login_required
def escalate(member_id):
    db = get_db()
    tag = request.form.get('escalation_tag', '').strip()
    note = request.form.get('escalation_note', '').strip()
    escalated_to = request.form.get('escalated_to', '')
    if not tag:
        flash('Escalation tag is required.', 'error')
        return redirect(url_for('member_detail', member_id=member_id))
    if not note:
        flash('Escalation note is required.', 'error')
        return redirect(url_for('member_detail', member_id=member_id))
    now = datetime.now().isoformat()
    db.execute("""UPDATE members SET escalated=1, escalation_tag=?, escalation_note=?,
                  escalated_by=?, escalated_to=?, escalated_at=?, updated_at=? WHERE id=?""",
               (tag, note, current_user(), escalated_to, now, now, member_id))
    add_history(db, member_id, f'Escalated to {tag}', note)
    db.commit()
    flash(f'Escalated with tag: {tag}.', 'warning')
    return redirect(url_for('member_detail', member_id=member_id))


@app.route('/member/<int:member_id>/de-escalate', methods=['POST'])
@login_required
def de_escalate(member_id):
    db = get_db()
    now = datetime.now().isoformat()
    db.execute("""UPDATE members SET escalated=0, escalation_tag='', escalation_note='',
                  escalated_by='', escalated_to='', escalated_at='', updated_at=? WHERE id=?""",
               (now, member_id))
    add_history(db, member_id, 'De-escalated')
    db.commit()
    flash('De-escalated.', 'success')
    return redirect(url_for('member_detail', member_id=member_id))


@app.route('/member/<int:member_id>/add-note', methods=['POST'])
@login_required
def add_note(member_id):
    db = get_db()
    note = request.form.get('note', '').strip()
    if note:
        now = datetime.now().isoformat()
        existing = db.execute("SELECT notes FROM members WHERE id=?", (member_id,)).fetchone()
        new_notes = f"{existing['notes']}\n[{now[:10]}] {current_user()}: {note}" if existing['notes'] else f"[{now[:10]}] {current_user()}: {note}"
        db.execute("UPDATE members SET notes=?, updated_at=? WHERE id=?", (new_notes, now, member_id))
        add_history(db, member_id, 'Added Note', note)
        db.commit()
        flash('Note added.', 'success')
    return redirect(url_for('member_detail', member_id=member_id))


@app.route('/member/<int:member_id>/set-workflow', methods=['POST'])
@login_required
def set_workflow(member_id):
    db = get_db()
    status = request.form.get('workflow_status')
    now = datetime.now().isoformat()
    db.execute("UPDATE members SET workflow_status=?, updated_at=? WHERE id=?", (status, now, member_id))
    add_history(db, member_id, f'Set Workflow Status: {status}')
    db.commit()
    flash(f'Workflow set to {status}.', 'success')
    return redirect(url_for('member_detail', member_id=member_id))


# --------------- Routes: Family ---------------

@app.route('/member/<int:member_id>/set-primary', methods=['POST'])
@login_required
def set_family_primary(member_id):
    db = get_db()
    m = db.execute("SELECT family_group_id FROM members WHERE id=?", (member_id,)).fetchone()
    if m and m['family_group_id']:
        now = datetime.now().isoformat()
        db.execute("UPDATE members SET family_role='Dependent' WHERE family_group_id=?", (m['family_group_id'],))
        db.execute("UPDATE members SET family_role='Primary', updated_at=? WHERE id=?", (now, member_id))
        add_history(db, member_id, 'Set as Family Primary')
        db.commit()
        flash('Set as family primary.', 'success')
    return redirect(url_for('member_detail', member_id=member_id))


@app.route('/member/<int:member_id>/unlink-family', methods=['POST'])
@login_required
def unlink_family(member_id):
    db = get_db()
    now = datetime.now().isoformat()
    db.execute("UPDATE members SET family_group_id=NULL, family_role='Unknown', updated_at=? WHERE id=?",
               (now, member_id))
    add_history(db, member_id, 'Unlinked from Family')
    db.commit()
    flash('Unlinked from family.', 'success')
    return redirect(url_for('member_detail', member_id=member_id))


@app.route('/link-family', methods=['POST'])
@login_required
def link_family():
    db = get_db()
    member_ids = request.form.getlist('member_ids')
    group_name = request.form.get('group_name', '').strip() or 'Family Group'
    if len(member_ids) < 2:
        flash('Select at least 2 members to link.', 'error')
        return redirect(request.referrer or url_for('queue'))
    now = datetime.now().isoformat()
    db.execute("INSERT INTO family_groups (group_name, created_at) VALUES (?, ?)", (group_name, now))
    gid = db.execute("SELECT last_insert_rowid()").fetchone()[0]
    for mid in member_ids:
        db.execute("UPDATE members SET family_group_id=?, updated_at=? WHERE id=?", (gid, now, int(mid)))
        add_history(db, int(mid), f'Linked to Family: {group_name}')
    db.commit()
    flash(f'Family group "{group_name}" created.', 'success')
    return redirect(request.referrer or url_for('queue'))


# --------------- Routes: Export ---------------

@app.route('/export')
@login_required
def export_data():
    db = get_db()
    members = db.execute("SELECT * FROM members ORDER BY id").fetchall()
    data = [dict(m) for m in members]
    return jsonify(data)


# --------------- Seed ---------------

@app.route('/seed', methods=['GET', 'POST'])
@login_required
def seed_page():
    if request.method == 'POST':
        seed_from_json()
        flash('Seed data imported successfully!', 'success')
        return redirect(url_for('dashboard'))
    db = get_db()
    count = db.execute("SELECT COUNT(*) FROM members").fetchone()[0]
    return render_template('seed.html', count=count, user=current_user())


def seed_from_json():
    json_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'spreadsheet_data.json')
    with open(json_path, 'r', encoding='utf-8') as f:
        all_data = json.load(f)

    db = sqlite3.connect(DATABASE)
    db.execute("PRAGMA foreign_keys=ON")
    now = datetime.now().isoformat()

    rows = all_data.get('Active Import', {}).get('rows', [])

    for row in rows:
        price_val = 0
        try:
            price_val = float(row.get('Price', 0) or 0)
        except (ValueError, TypeError):
            pass
        monthly_val = 0
        try:
            monthly_val = float(row.get('Monthly Invoice', 0) or 0)
        except (ValueError, TypeError):
            pass

        nbd = row.get('Next Billing Date', '') or ''
        if nbd and ' ' in nbd:
            nbd = nbd.split(' ')[0]
        ndd = row.get('Next Due Date', '') or ''
        if ndd and ' ' in ndd:
            ndd = ndd.split(' ')[0]

        db.execute("""INSERT INTO members (
            member_number, agreement_number, member_name, first_name, last_name,
            address, city, state, zip, best_phone, email_address, date_of_birth, gender,
            membership_type, payment_plan, payment_mode, price, price_bucket,
            payment_category, monthly_invoice, start_date, last_billed_date,
            next_billing_date, next_due_date, conversion_priority, recommendation,
            workflow_status, abc_status, gymdesk_status, gymdesk_outcome,
            gymdesk_checked, abc_turned_off, attempt_count,
            created_at, updated_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (
            row.get('Member #', ''),
            row.get('Agreement #', ''),
            row.get('Member Name', ''),
            row.get('First Name', ''),
            row.get('Last Name', ''),
            row.get('Address', ''),
            row.get('City', ''),
            row.get('State', ''),
            row.get('Zip', '').strip(),
            row.get('Best Phone', ''),
            row.get('Email Address', ''),
            row.get('Date of Birth', ''),
            row.get('Gender', ''),
            row.get('Membership Type', ''),
            row.get('Payment Plan', ''),
            row.get('Payment Mode', ''),
            price_val,
            row.get('Price Bucket', ''),
            row.get('Payment Category', ''),
            monthly_val,
            row.get('Start Date', ''),
            row.get('Last Billed Date', ''),
            nbd,
            ndd,
            row.get('Conversion Priority', ''),
            row.get('Recommendation', ''),
            'Pending Outreach',
            'Active',
            'Pending',
            'Pending',
            0, 0, 0,
            now, now
        ))

    db.commit()

    # Auto-group families
    auto_group_families(db)
    db.close()


def auto_group_families(db):
    """Auto-group families by phone, email, or last_name+address."""
    now = datetime.now().isoformat()
    members = db.execute("SELECT id, first_name, last_name, best_phone, email_address, address FROM members WHERE family_group_id IS NULL").fetchall()

    phone_groups = {}
    email_groups = {}
    addr_groups = {}

    for m in members:
        mid, fn, ln, phone, email, addr = m
        if phone and phone.strip():
            phone_groups.setdefault(phone.strip(), []).append(mid)
        if email and email.strip():
            email_groups.setdefault(email.strip().lower(), []).append(mid)
        if ln and addr and ln.strip() and addr.strip():
            key = (ln.strip().upper(), addr.strip().upper())
            addr_groups.setdefault(key, []).append(mid)

    assigned = set()

    def create_group(ids, name):
        if len(ids) < 2:
            return
        unassigned = [i for i in ids if i not in assigned]
        if len(unassigned) < 2:
            return
        db.execute("INSERT INTO family_groups (group_name, created_at) VALUES (?, ?)", (name, now))
        gid = db.execute("SELECT last_insert_rowid()").fetchone()[0]
        for mid in unassigned:
            db.execute("UPDATE members SET family_group_id=?, family_role='Unknown' WHERE id=?", (gid, mid))
            assigned.add(mid)

    for phone, ids in phone_groups.items():
        if len(ids) >= 2:
            first = db.execute("SELECT last_name FROM members WHERE id=?", (ids[0],)).fetchone()
            name = f"{first[0]} Family" if first else f"Phone {phone}"
            create_group(ids, name)

    for email, ids in email_groups.items():
        if len(ids) >= 2:
            first = db.execute("SELECT last_name FROM members WHERE id=?", (ids[0],)).fetchone()
            name = f"{first[0]} Family" if first else f"Email Group"
            create_group(ids, name)

    for (ln, addr), ids in addr_groups.items():
        if len(ids) >= 2:
            create_group(ids, f"{ln.title()} Family")

    db.commit()


# --------------- Init & Run ---------------

def _startup():
    """Initialize DB, run migrations, and auto-seed. Called at module import so
    it runs under both `python app.py` (direct) and gunicorn worker startup."""
    init_db()
    # Normalize next_billing_date to strip time component from existing records
    _db = sqlite3.connect(DATABASE)
    _db.execute("""
        UPDATE members
        SET next_billing_date = SUBSTR(next_billing_date, 1, 10)
        WHERE next_billing_date LIKE '____-__-__ %'
    """)
    _db.commit()
    count = _db.execute("SELECT COUNT(*) FROM members").fetchone()[0]
    _db.close()
    if count == 0:
        print("Database empty — auto-seeding from spreadsheet_data.json...")
        seed_from_json()
        print("Seed complete.")


_startup()


if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True, port=5000)
