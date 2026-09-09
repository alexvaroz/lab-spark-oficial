#!/usr/bin/env bash
#
# setup.sh — Sobe o cluster Hadoop + Spark (bde2020) no Codespaces, sempre do
# jeito certo: rede compartilhada configurada ANTES do docker-compose up,
# evitando o erro "UnknownHostException: namenode".
#
# Uso:
#   chmod +x setup.sh
#   ./setup.sh
#
# Pode rodar mais de uma vez sem problema (é idempotente): se os repositórios
# já estiverem clonados, não clona de novo; se a rede já existir, não recria;
# se os containers já estiverem de pé, o `docker-compose up -d` só garante
# que continuam no ar.

set -uo pipefail

REDE="hadoop_spark_net"
CSV="nyc_taxi_trip_2024_p1_sample.csv"
SCRIPT_SPARK="payment_type.py"

echo "==> 1/5 Verificando o Docker..."
if ! docker info > /dev/null 2>&1; then
  echo "ERRO: o daemon do Docker não respondeu. No Codespaces, confirme que a"
  echo "feature 'docker-in-docker' está no .devcontainer/devcontainer.json e"
  echo "reconstrua o container (Codespaces -> Rebuild Container)."
  exit 1
fi

echo "==> 2/5 Criando a rede compartilhada '$REDE' (se ainda não existir)..."
if docker network inspect "$REDE" > /dev/null 2>&1; then
  echo "    Rede '$REDE' já existe, seguindo."
else
  docker network create "$REDE"
  echo "    Rede '$REDE' criada."
fi

echo "==> 3/5 Clonando docker-hadoop e docker-spark (se necessário)..."
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

echo "==> 4/5 Subindo os clusters..."
( cd docker-hadoop && docker-compose up -d )
( cd docker-spark  && docker-compose up -d )

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

echo "==> 5/5 Verificando se Spark enxerga o Hadoop pela rede..."
if docker exec spark-master getent hosts namenode > /dev/null 2>&1; then
  echo "    OK: spark-master resolve 'namenode' normalmente."
else
  echo "AVISO: spark-master ainda não resolve 'namenode'. Rode:"
  echo "  docker exec -it spark-master getent hosts namenode"
  echo "para investigar (pode ser só questão de mais alguns segundos de boot)."
fi

# --- Passos opcionais: só rodam se os arquivos existirem no repositório ---

if [ -f "$CSV" ]; then
  echo "==> Extra: enviando '$CSV' para o HDFS..."
  docker cp "$CSV" namenode:/tmp/
  docker exec namenode hdfs dfs -mkdir -p /dados
  docker exec namenode hdfs dfs -put -f "/tmp/$CSV" /dados/
  echo "    Arquivo disponível em hdfs://namenode:9000/dados/$CSV"
else
  echo "==> Aviso: '$CSV' não encontrado no diretório atual -- pulei o upload pro HDFS."
fi

if [ -f "$SCRIPT_SPARK" ]; then
  echo "==> Extra: copiando '$SCRIPT_SPARK' para o spark-master..."
  docker cp "$SCRIPT_SPARK" "spark-master:/$SCRIPT_SPARK"
  echo "    Pronto. Para rodar:"
  echo "    docker exec -it spark-master /spark/bin/spark-submit --master spark://spark-master:7077 /$SCRIPT_SPARK"
else
  echo "==> Aviso: '$SCRIPT_SPARK' não encontrado no diretório atual -- pulei a cópia."
fi

echo ""
echo "==> Cluster no ar. Interfaces web (aba 'Ports' do Codespace):"
echo "    Namenode (HDFS)         -> porta 9870"
echo "    ResourceManager (YARN)  -> porta 8088"
echo "    Spark Master            -> porta 8080"