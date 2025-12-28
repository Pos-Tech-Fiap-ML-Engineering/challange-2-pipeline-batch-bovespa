Aqui está a sua documentação completa e atualizada. Adicionei a **Seção 6**, que cobre a configuração de permissões na AWS (IAM), a instalação das bibliotecas e o código da DAG para o projeto financeiro.

---

# 📘 Airflow na AWS EC2: Guia Completo

**Objetivo:** Criar um ambiente Airflow acessível, com IP fixo, rodando em background e executar um pipeline de dados financeiros (ETL).
**Custo Estimado:** Gratuito (Free Tier 12 meses) ou ~$10/mês.

---

## 1. Infraestrutura AWS (Console)

### 1.1. Criar a Instância EC2

1. Acesse o **Console EC2** e clique em **Launch Instance**.
2. **Nome:** `Airflow-Estudos`.
3. **AMI (Sistema Operacional):** Ubuntu Server 24.04 LTS (ou 22.04).
4. **Instance Type:** `t2.micro` ou `t3.micro` (Verifique a tag "Free tier eligible").
5. **Key Pair:** Crie um novo par `.pem`, baixe e guarde em local seguro.
6. **Network Settings:**
* Crie um novo **Security Group**.
* **Inbound Rules (Entrada):**
* `SSH (22)` -> `My IP` (Para sua segurança) ou `0.0.0.0/0`.
* `Custom TCP (8080)` -> `0.0.0.0/0` (Para acessar o Airflow).




7. **Storage:** Pode deixar o padrão (8GB gp3).
8. **Launch Instance**.

### 1.2. Configurar IP Fixo (Elastic IP)

Para que o IP não mude ao reiniciar a máquina.

1. No menu esquerdo do EC2, vá em **Network & Security** > **Elastic IPs**.
2. Clique em **Allocate Elastic IP address** > **Allocate**.
3. Selecione o IP criado, clique em **Actions** > **Associate Elastic IP address**.
4. Selecione sua instância e o Private IP dela. Clique em **Associate**.
5. **Anote este IP** (chamaremos de `SEU_IP_ELASTICO`).

### 1.3. Ajuste Fino de Segurança (Crucial)

1. Vá em **Security Groups** > Selecione o grupo criado.
2. Vá na aba **Outbound rules** (Regras de saída).
3. Verifique se existe uma regra liberando **All traffic** para `0.0.0.0/0`. Se não houver, adicione-a (sem isso o servidor não baixa pacotes).

---

## 2. Configuração do Sistema (Terminal)

Acesse via SSH (no Windows/Powershell):

```bash
ssh -i "sua-chave.pem" ubuntu@SEU_IP_ELASTICO

```

### 2.1. Criar Swap de Memória

Como a máquina tem apenas 1GB de RAM, o Swap evita travamentos.

```bash
# Criar e ativar 2GB de Swap
sudo fallocate -l 2G /swapfile
sudo chmod 600 /swapfile
sudo mkswap /swapfile
sudo swapon /swapfile

# Tornar permanente após reinicialização
echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab

```

### 2.2. Instalar Dependências e Airflow

```bash
# Atualizar sistema
sudo apt update && sudo apt upgrade -y

# Instalar Python Pip e Venv
sudo apt install -y python3-pip python3-venv

# Criar ambiente virtual na pasta home
python3 -m venv airflow_env

# Ativar ambiente
source airflow_env/bin/activate

# Instalar Airflow (Versão estável)
pip install "apache-airflow==2.10.2" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.2/constraints-3.12.txt"

# 2. Instalação com constraints (Padrão Ouro)
pip install "apache-airflow-providers-amazon" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.2/constraints-3.12.txt"

```

---

## 3. Automatização (Systemd Service)

Configurar o Airflow para rodar sozinho em background e reiniciar em caso de falhas.

### 3.1. Criar o arquivo de serviço

```bash
sudo nano /etc/systemd/system/airflow.service

```

### 3.2. Colar a configuração

Copie e cole o conteúdo abaixo:

```ini
[Unit]
Description=Airflow Standalone Service
After=network.target

[Service]
User=ubuntu
Group=ubuntu
Type=simple
WorkingDirectory=/home/ubuntu
# Define o PATH para incluir o bin do ambiente virtual
Environment="PATH=/home/ubuntu/airflow_env/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
ExecStart=/home/ubuntu/airflow_env/bin/airflow standalone
Restart=always
RestartSec=5s

[Install]
WantedBy=multi-user.target

```

*Salve com `Ctrl+O`, `Enter`, `Ctrl+X`.*

### 3.3. Ativar o Serviço

```bash
sudo systemctl daemon-reload
sudo systemctl enable airflow.service
sudo systemctl start airflow.service

```

### 3.4. Reiniciar o serviço (Se necessário)
```bash
sudo systemctl restart airflow.service
```

---

## 4. Gerenciamento de Usuários

### 4.1. Criar Usuário "Visitante" (Somente Leitura)

```bash
source ~/airflow_env/bin/activate

airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password zWQwZrmhx3aGXNsy

airflow users create \
    --username visitante \
    --firstname Visitante \
    --lastname Leitor \
    --role Viewer \
    --email visitante@exemplo.com
    --password DHA+F[?6q(

```

### 4.2. Recuperar Senha de Admin (Opcional)

```bash
sudo journalctl -u airflow.service | grep "User" -A 5

```

---

## 5. Como Acessar

* **URL:** `http://SEU_IP_ELASTICO:8080`
* **Login Admin:** `admin` / (Senha recuperada no passo 4.2)
* **Login Visitante:** `visitante` / (Senha definida no passo 4.1)

---

## 6. Projeto Prático: ETL B3 para S3

### 6.1. Configurar Permissões AWS (IAM)

Para o Airflow escrever no S3 sem expor senhas no código:

1. No **AWS Console**, vá em **IAM** > **Roles** > **Create role**.
2. Select trusted entity: **AWS Service** > **EC2**.
3. Permissions: Procure e selecione `AmazonS3FullAccess` (ou crie uma política específica para seu bucket).
4. Dê um nome (ex: `Role-EC2-Airflow-S3`) e crie.
5. Vá no **Console EC2** > Selecione sua instância (Airflow-Estudos).
6. **Actions** > **Security** > **Modify IAM role**.
7. Selecione a role criada e clique em **Update IAM role**.

### 6.2. Instalar Bibliotecas do Projeto

No terminal da EC2:

```bash
source ~/airflow_env/bin/activate

pip install s3fs --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.2/constraints-3.12.txt"

# 1. Alinha as bibliotecas estruturais com a versão do Airflow
pip install pandas pyarrow --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.2/constraints-3.12.txt"

# 2. Instala o yfinance (que não faz parte do core do Airflow)
pip install yfinance

# Reinicie o Airflow para garantir que ele reconheça as novas libs
sudo systemctl restart airflow.service

```

### 6.3. Criar a DAG

1. Crie o bucket no S3 (ex: `meu-datalake-b3-estudos`).
2. Crie a arquivo da DAG

```bash
# 1. Cria a pasta dags dentro do diretório do airflow
mkdir -p ~/airflow/dags

# 2. Agora sim, entra nela
cd ~/airflow/dags

# 3. Cria o arquivo da DAG
nano dag_b3_backfill_complete.py
```

---

## 💡 Dicas de Manutenção

* **Para ver logs em tempo real:**
`journalctl -u airflow.service -f`
* **Para parar o Airflow:**
`sudo systemctl stop airflow.service`
* **Para verificar status do serviço:**
`sudo systemctl status airflow.service`
* **Exclusão de usuário admin (Se necessário)**:
`airflow users delete --username admin`
* **Comando airflow para atualizar list das dags:**
`python3 ~/airflow/dags/{{DAG_FILE}}.pyt` <br>
`airflow dags list` <br>
`airflow dags reserialize` <br>
`airflow dags unpause {{DAG}}` <br>
`airflow dags pause {{DAG}}` <br>
`airflow dags delete {{DAG}} -y` <br>
`rm ~/airflow/dags/__pycache__/` <br>
* **Para economizar:**
Se não estiver usando, dê **Stop Instance** na AWS.
* **Atenção:** Lembre-se que IP Elástico parado **cobra** uma pequena taxa se não estiver associado a uma instância rodando. Se for abandonar o projeto, libere o IP (`Release`) e encerre a instância (`Terminate`).