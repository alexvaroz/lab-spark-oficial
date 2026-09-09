#!/usr/bin/env bash
#
# setup.sh — Sobe o cluster Hadoop + Spark (bde2020) no Codespaces, sempre do
# jeito certo:
#   1. Corrige a política da chain FORWARD do iptables quando estiver em DROP
#      (comum em ambientes Docker-in-Docker como o Codespaces, e que impede
#      containers de se conectarem entre si mesmo estando na mesma rede).
#      A correção é mínima por design: ajusta só a política, sem trocar o
#      backend padrão do iptables do sistema nem reiniciar o Docker -- versões
#      recentes do Docker gerenciam cadeias próprias (DOCKER-FORWARD) que
#      dependem especificamente do backend nft, e uma correção mais agressiva
#      (trocar para legacy + reiniciar o Docker) pode quebrar essa gestão
#      interna em alguns ambientes.
#   2. Cria a rede compartilhada ANTES do docker-compose up, evitando o erro
#      "UnknownHostException: namenode" entre os dois projetos separados.
#
# Uso:
#   chmod +x setup.sh
#   ./setup.sh
#
# Pode rodar mais de uma vez sem problema (é idempotente): se os repositórios
# já estiverem clonados, não clona de novo; se a rede já existir, não recria;
# se os containers já estiverem de pé, o `docker-compose up -d` só garante
# que continuam no ar; se a rede do SO já estiver correta, não mexe em nada.

set -uo pipefail

REDE="hadoop_spark_net"
CSV="nyc_tripdata_2024_sample_200k.csv"
SCRIPT_SPARK="payment_type.py"

# ------------------------------------------------------------------------
# 1/6 — Corrigir problemas de rede do SO (ip_forward / iptables)
# ------------------------------------------------------------------------
echo "==> 1/6 Verificando compatibilidade de rede do host (ip_forward / iptables)..."

if [ "$(sysctl -n net.ipv4.ip_forward 2>/dev/null)" != "1" ]; then
  echo "    net.ipv4.ip_forward estava desligado -- ligando."
  sudo sysctl -w net.ipv4.ip_forward=1 > /dev/null
else
  echo "    net.ipv4.ip_forward já está ligado."
fi

PRECISA_AVISAR_RESET_MANUAL=false

# Confere a política da chain FORWARD nos dois conjuntos de regras possíveis
# (iptables "moderno"/nft e iptables-legacy). Em ambientes Docker-in-Docker
# como o Codespaces, os dois podem coexistir -- e o kernel pode aplicar um
# conjunto diferente do que `iptables` (sem sufixo) está mostrando, fazendo
# com que uma política DROP passe despercebida numa checagem simples.
#
# IMPORTANTE: corrigimos a política (-P FORWARD ACCEPT) diretamente em cada
# conjunto, SEM trocar qual conjunto é o "padrão do sistema"
# (update-alternatives) e SEM reiniciar o Docker. Versões recentes do Docker
# gerenciam cadeias próprias (ex: DOCKER-FORWARD) esperando especificamente o
# backend nft -- forçar o sistema inteiro para o legacy quebra essa gestão
# interna do próprio Docker em alguns ambientes (erro visto: "No chain/target
# /match by that name" ao tentar recriar DOCKER-FORWARD). Ajustar só a
# política, sem trocar o binário padrão, é a correção mínima e segura.
for BIN in iptables iptables-legacy; do
  if command -v "$BIN" > /dev/null 2>&1; then
    POLICY=$(sudo "$BIN" -L FORWARD -n 2>/dev/null | head -1 | sed -n 's/.*(policy \([A-Za-z]*\).*/\1/p')
    if [ "$POLICY" = "DROP" ] || [ "$POLICY" = "REJECT" ]; then
      echo "    $BIN: chain FORWARD com política $POLICY -- isso bloqueia tráfego"
      echo "    entre containers em redes diferentes. Corrigindo para ACCEPT..."
      sudo "$BIN" -P FORWARD ACCEPT
      PRECISA_AVISAR_RESET_MANUAL=true
    elif [ -n "$POLICY" ]; then
      echo "    $BIN: chain FORWARD já está com política $POLICY, ok."
    fi
  fi
done

if [ "$PRECISA_AVISAR_RESET_MANUAL" = true ]; then
  echo "    Política(s) corrigida(s). Isso normalmente já é suficiente -- o"
  echo "    script NÃO reinicia o Docker nem troca o backend padrão de"
  echo "    iptables, pois isso pode quebrar a gestão interna de rede de"
  echo "    versões recentes do Docker. Se, mesmo assim, os containers não"
  echo "    se conectarem entre si mais adiante, rode 'sudo service docker"
  echo "    restart' manualmente e tente de novo."
fi

# ------------------------------------------------------------------------
# 2/6 — Verificar o Docker
# ------------------------------------------------------------------------
echo "==> 2/6 Verificando o Docker..."
if ! docker info > /dev/null 2>&1; then
  echo "ERRO: o daemon do Docker não respondeu. No Codespaces, confirme que a"
  echo "feature 'docker-in-docker' está no .devcontainer/devcontainer.json e"
  echo "reconstrua o container (Codespaces -> Rebuild Container)."
  exit 1
fi

# ------------------------------------------------------------------------
# 3/6 — Rede compartilhada entre os dois docker-compose
# ------------------------------------------------------------------------
echo "==> 3/6 Criando a rede compartilhada '$REDE' (se ainda não existir)..."
if docker network inspect "$REDE" > /dev/null 2>&1; then
  echo "    Rede '$REDE' já existe, seguindo."
else
  TENTATIVAS=0
  until docker network create "$REDE" > /dev/null 2>&1; do
    TENTATIVAS=$((TENTATIVAS + 1))
    if [ "$TENTATIVAS" -ge 5 ]; then
      echo "ERRO: não consegui criar a rede '$REDE' depois de $TENTATIVAS tentativas."
      echo "Rode 'docker network create $REDE' manualmente para ver a mensagem de erro exata."
      exit 1
    fi
    echo "    Falha ao criar a rede (tentativa $TENTATIVAS/5) -- tentando de novo em 2s..."
    sleep 2
  done
  echo "    Rede '$REDE' criada."
fi

# Confirma de verdade que a rede existe antes de seguir -- não custa nada e
# evita prosseguir com uma suposição incorreta.
if ! docker network inspect "$REDE" > /dev/null 2>&1; then
  echo "ERRO: a rede '$REDE' deveria existir agora, mas 'docker network inspect' não a encontrou."
  echo "Rode 'docker network ls' para investigar antes de continuar."
  exit 1
fi

# ------------------------------------------------------------------------
# 4/6 — Clonar os repositórios (se necessário) e aplicar o override de rede
# ------------------------------------------------------------------------
echo "==> 4/6 Clonando docker-hadoop e docker-spark (se necessário)..."
for REPO in docker-hadoop docker-spark; do
  if [ -f "$REPO/docker-compose.yml" ]; then
    echo "    '$REPO' já está presente, seguindo."
  else
    echo "    '$REPO' ausente ou incompleto — (re)clonando..."
    rm -rf "$REPO"
    git clone --depth 1 "https://github.com/big-data-europe/$REPO.git"
  fi

  # Garante que o override de rede está presente, mesmo em clones antigos
  cat > "$REPO/docker-compose.override.yml" << EOF
networks:
  default:
    name: $REDE
    external: true
EOF
done

# A branch atual do docker-spark define só 1 worker (spark-worker-1) por padrão.
# Para atividades que precisam de mais de um worker (ex: demonstrar tolerância
# a falhas matando um worker no meio de um job), adicionamos um segundo aqui,
# reaproveitando a mesma imagem e variáveis de ambiente do primeiro.
if ! grep -q "spark-worker-2" docker-spark/docker-compose.override.yml 2>/dev/null; then
  IMAGEM_WORKER=$(grep -A1 "^  spark-worker-1:" docker-spark/docker-compose.yml | grep "image:" | awk '{print $2}')
  cat >> docker-spark/docker-compose.override.yml << EOF
services:
  spark-worker-2:
    image: ${IMAGEM_WORKER}
    container_name: spark-worker-2
    depends_on:
      - spark-master
    ports:
      - "8082:8081"
    environment:
      - SPARK_MASTER=spark://spark-master:7077
EOF
fi

# ------------------------------------------------------------------------
# 5/6 — Subir os clusters e validar
# ------------------------------------------------------------------------
echo "==> 5/6 Subindo os clusters..."
( cd docker-hadoop && docker-compose up -d )
if [ $? -ne 0 ]; then
  echo "ERRO: falha ao subir o docker-hadoop (veja a mensagem acima)."
  echo "Confira 'docker network ls' -- a rede '$REDE' precisa existir antes deste passo."
  exit 1
fi

( cd docker-spark && docker-compose up -d )
if [ $? -ne 0 ]; then
  echo "ERRO: falha ao subir o docker-spark (veja a mensagem acima)."
  echo "Confira 'docker network ls' -- a rede '$REDE' precisa existir antes deste passo."
  exit 1
fi

echo "    Aguardando o namenode responder..."
TENTATIVAS=0
until docker exec namenode hdfs dfs -ls / > /dev/null 2>&1; do
  TENTATIVAS=$((TENTATIVAS + 1))
  if [ "$TENTATIVAS" -ge 20 ]; then
    echo "AVISO: o namenode não respondeu depois de várias tentativas."
    echo "Rode 'docker logs namenode' para investigar antes de continuar."
    break
  fi
  sleep 3
done

echo "    Verificando se Spark enxerga o Hadoop pela rede..."
if docker exec spark-master getent hosts namenode > /dev/null 2>&1; then
  echo "    OK: spark-master resolve 'namenode' normalmente."
else
  echo "AVISO: spark-master ainda não resolve 'namenode'. Rode:"
  echo "  docker exec -it spark-master getent hosts namenode"
  echo "para investigar (pode ser só questão de mais alguns segundos de boot)."
fi

echo "    Aguardando ao menos um worker Spark se conectar ao master..."
TENTATIVAS=0
until docker exec spark-master curl -s http://localhost:8080 2>/dev/null | grep -q "Alive Workers: [1-9]"; do
  TENTATIVAS=$((TENTATIVAS + 1))
  if [ "$TENTATIVAS" -ge 15 ]; then
    echo "AVISO: nenhum worker apareceu como 'Alive' na UI do Spark Master ainda."
    echo "Se isso persistir, confira 'docker logs spark-worker-1' -- um sintoma"
    echo "comum é 'Retrying connection to master' em loop, que geralmente é o"
    echo "mesmo problema de iptables corrigido no passo 1/6 (raro acontecer de"
    echo "novo depois da correção, mas pode acontecer em outro Codespace)."
    break
  fi
  sleep 4
done
if [ "$TENTATIVAS" -lt 15 ]; then
  echo "    OK: pelo menos um worker Spark está conectado."
fi

# ------------------------------------------------------------------------
# 6/6 — Passos opcionais: só rodam se os arquivos existirem no repositório
# ------------------------------------------------------------------------
echo "==> 6/6 Preparando dados e script de exemplo (se presentes)..."

if [ -f "$CSV" ]; then
  echo "    Enviando '$CSV' para o HDFS..."
  docker cp "$CSV" namenode:/tmp/
  docker exec namenode hdfs dfs -mkdir -p /dados
  docker exec namenode hdfs dfs -put -f "/tmp/$CSV" /dados/
  echo "    Arquivo disponível em hdfs://namenode:9000/dados/$CSV"
else
  echo "    Aviso: '$CSV' não encontrado no diretório atual -- pulei o upload pro HDFS."
fi

if [ -f "$SCRIPT_SPARK" ]; then
  echo "    Copiando '$SCRIPT_SPARK' para o spark-master..."
  docker cp "$SCRIPT_SPARK" "spark-master:/$SCRIPT_SPARK"
  echo "    Pronto. Para rodar:"
  echo "    docker exec -it spark-master /spark/bin/spark-submit --master spark://spark-master:7077 /$SCRIPT_SPARK"
else
  echo "    Aviso: '$SCRIPT_SPARK' não encontrado no diretório atual -- pulei a cópia."
fi

echo ""
echo "==> Cluster no ar. Interfaces web (aba 'Ports' do Codespace):"
echo "    Namenode (HDFS)         -> porta 9870"
echo "    ResourceManager (YARN)  -> porta 8088"
echo "    Spark Master            -> porta 8080"
