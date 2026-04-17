"""AdPilot 一键部署脚本"""
import paramiko
import os
import stat
import time

HOST = "43.103.5.32"
USER = "root"
PASSWD = "+X3TR@zggQtkD&x"
REMOTE_DIR = "/opt/adpilot"
LOCAL_DIR = r"D:\AI ADS"

SKIP_DIRS = {"__pycache__", ".git", "node_modules", "terminals", ".cursor", "agent-transcripts", "mcps", ".env.example"}
SKIP_FILES = {"deploy.py", "test_connection.py", "oplog.json", "users.json"}
INCLUDE_EXTS = {".py", ".html", ".css", ".js", ".txt", ".env", ".json"}


def ssh_connect():
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(HOST, username=USER, password=PASSWD, timeout=15)
    return client


def run_cmd(client, cmd, show=True):
    stdin, stdout, stderr = client.exec_command(cmd, timeout=120)
    out = stdout.read().decode("utf-8", errors="replace")
    err = stderr.read().decode("utf-8", errors="replace")
    if show:
        if out.strip():
            print(f"  [OUT] {out.strip()[:300]}")
        if err.strip():
            print(f"  [ERR] {err.strip()[:300]}")
    return out, err


def should_upload(filepath, relpath):
    parts = relpath.replace("\\", "/").split("/")
    for part in parts:
        if part in SKIP_DIRS:
            return False
    fname = os.path.basename(filepath)
    if fname in SKIP_FILES:
        return False
    _, ext = os.path.splitext(fname)
    if ext and ext not in INCLUDE_EXTS:
        return False
    if not ext and fname not in {".env"}:
        return False
    return True


def upload_project(client):
    sftp = client.open_sftp()
    uploaded = 0

    for root, dirs, files in os.walk(LOCAL_DIR):
        dirs[:] = [d for d in dirs if d not in SKIP_DIRS]
        for fname in files:
            local_path = os.path.join(root, fname)
            rel_path = os.path.relpath(local_path, LOCAL_DIR)
            if not should_upload(local_path, rel_path):
                continue
            remote_path = f"{REMOTE_DIR}/{rel_path}".replace("\\", "/")
            remote_dir = os.path.dirname(remote_path).replace("\\", "/")

            try:
                sftp.stat(remote_dir)
            except FileNotFoundError:
                run_cmd(client, f"mkdir -p {remote_dir}", show=False)

            sftp.put(local_path, remote_path)
            uploaded += 1
            print(f"  [{uploaded}] {rel_path}")

    sftp.close()
    return uploaded


def main():
    print("=" * 50)
    print("  AdPilot 部署脚本")
    print("=" * 50)

    print("\n[1/6] 连接服务器...")
    client = ssh_connect()
    print(f"  已连接 {HOST}")

    print("\n[2/6] 安装系统依赖...")
    run_cmd(client, "apt-get update -qq && apt-get install -y -qq python3 python3-pip python3-venv nginx > /dev/null 2>&1")
    print("  Python3 + Nginx 已安装")

    print("\n[3/6] 创建项目目录并上传文件...")
    run_cmd(client, f"mkdir -p {REMOTE_DIR}", show=False)
    count = upload_project(client)
    print(f"  共上传 {count} 个文件")

    print("\n[4/6] 创建虚拟环境并安装依赖...")
    run_cmd(client, f"cd {REMOTE_DIR} && python3 -m venv venv && venv/bin/pip install --upgrade pip -q")
    run_cmd(client, f"cd {REMOTE_DIR} && venv/bin/pip install -r requirements.txt -q")
    print("  依赖安装完成")

    print("\n[5/6] 配置 systemd 服务...")
    service_content = f"""[Unit]
Description=AdPilot Ad Management System
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory={REMOTE_DIR}
ExecStart={REMOTE_DIR}/venv/bin/python app.py
Restart=always
RestartSec=5
Environment=PATH={REMOTE_DIR}/venv/bin:/usr/bin:/bin

[Install]
WantedBy=multi-user.target
"""
    run_cmd(client, f"cat > /etc/systemd/system/adpilot.service << 'HEREDOC'\n{service_content}HEREDOC", show=False)

    nginx_conf = """server {
    listen 80;
    server_name _;

    location / {
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_read_timeout 300s;
    }
}
"""
    run_cmd(client, f"cat > /etc/nginx/sites-available/adpilot << 'HEREDOC'\n{nginx_conf}HEREDOC", show=False)
    run_cmd(client, "ln -sf /etc/nginx/sites-available/adpilot /etc/nginx/sites-enabled/adpilot", show=False)
    run_cmd(client, "rm -f /etc/nginx/sites-enabled/default", show=False)
    run_cmd(client, "nginx -t", show=True)
    print("  服务配置完成")

    print("\n[6/6] 启动服务...")
    run_cmd(client, "systemctl daemon-reload")
    run_cmd(client, "systemctl enable adpilot")
    run_cmd(client, "systemctl restart adpilot")
    run_cmd(client, "systemctl restart nginx")
    time.sleep(3)
    out, _ = run_cmd(client, "systemctl is-active adpilot")
    status = out.strip()
    if status == "active":
        print(f"\n{'=' * 50}")
        print(f"  部署成功!")
        print(f"  访问地址: http://{HOST}")
        print(f"  默认账号: admin / admin123")
        print(f"{'=' * 50}")
    else:
        print(f"\n  服务状态: {status}")
        print("  查看日志:")
        run_cmd(client, "journalctl -u adpilot --no-pager -n 30")

    # open firewall
    run_cmd(client, "ufw allow 80/tcp 2>/dev/null; ufw allow 443/tcp 2>/dev/null; ufw allow 22/tcp 2>/dev/null", show=False)

    client.close()


if __name__ == "__main__":
    main()
