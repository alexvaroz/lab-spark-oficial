# Guia de Execução — Laboratório de Processamento Distribuído com Hadoop e Spark

## Sobre este material

Este material foi desenvolvido para a disciplina **Processamento de Dados Massivos**, do curso de **Ciência de Dados e Inteligência Artificial** do **IESB**.

Ele documenta um laboratório prático de computação distribuída, montado inteiramente em containers Docker rodando em um ambiente de nuvem gratuito (GitHub Codespaces), sem exigir instalação local nem permissões de administrador na máquina do aluno.

## Objetivos

Ao final deste laboratório, o aluno deve ser capaz de:

- Subir, a partir do zero, um cluster real de Hadoop (HDFS + YARN) e Spark, usando containers Docker orquestrados por `docker-compose`.
- Identificar os papéis dos diferentes componentes de um cluster Hadoop/Spark (`namenode`, `datanode`, `resourcemanager`, `nodemanager`, `spark-master`, `spark-worker`) e explicar por que cada um existe.
- Entender, na prática (não só na teoria), como o HDFS distribui arquivos em **blocos** e como a **replicação** protege os dados contra a falha de um nó.
- Observar e explicar os limites reais da tolerância a falhas de um sistema distribuído — incluindo a existência de uma janela de tempo entre "um nó falhar" e "o sistema saber que o nó falhou".
- Submeter e executar um programa PySpark real em um cluster distribuído, e relacionar essa execução com o paradigma MapReduce estudado anteriormente na disciplina, em uma implementação manual.
- Diagnosticar e corrigir problemas comuns de configuração e rede em ambientes de cluster containerizados.

---

## Pré-requisitos

- Conta gratuita no [GitHub](https://github.com/signup) (para o Codespaces).
- Um repositório próprio (mesmo que vazio) contendo:
  - `.devcontainer/devcontainer.json`, com a feature `docker-in-docker` habilitada;
  - o script `setup.sh` (fornecido junto com este guia);
  - opcionalmente, `payment_type.py` e um arquivo de dados, para a Parte 4.

```json
{
  "image": "mcr.microsoft.com/devcontainers/base:ubuntu",
  "features": {
    "ghcr.io/devcontainers/features/docker-in-docker:2": {}
  }
}
```

---

## Parte 1 — Preparando o ambiente

Todo o cluster é montado a partir de dois repositórios públicos mantidos pela comunidade **Big Data Europe** (`bde2020`), originalmente um projeto de pesquisa financiado pela União Europeia (2015–2017) — hoje um padrão de fato para laboratórios didáticos de Hadoop/Spark em containers.

O script `setup.sh` automatiza toda a preparação, incorporando um conjunto de correções descobertas ao longo do desenvolvimento deste laboratório (detalhadas na tabela da Parte 6). Ele:

1. Corrige a política da chain `FORWARD` do `iptables`, quando necessário (comum em ambientes Docker-in-Docker como o Codespaces).
2. Cria uma rede Docker compartilhada (`hadoop_spark_net`) **antes** de subir qualquer container, para que os dois clusters (Hadoop e Spark) se enxerguem por nome desde o início.
3. Clona `docker-hadoop` e `docker-spark`, aplicando o *override* de rede.
4. Aplica uma correção preventiva no HDFS (resolução de hostname), evitando erros de leitura/escrita durante o laboratório.
5. Adiciona um segundo worker Spark.
6. Sobe os dois clusters e valida que o namenode, o datanode e ao menos um worker Spark estão de pé antes de encerrar.

**Para executar:**

```bash
chmod +x setup.sh
./setup.sh
```

Ao final, a saída deve confirmar `OK` para o namenode, o(s) datanode(s) e o(s) worker(s) Spark.

**Encaminhe as seguintes portas** (aba **Ports** do Codespace):

| Porta | Serviço |
|---|---|
| 9870 | HDFS Namenode (interface web) |
| 8088 | YARN ResourceManager (interface web) |
| 8080 | Spark Master (interface web) |
| 8081 / 8082 | Spark Worker 1 / 2 (interface web) |

---

## Parte 2 — Conhecendo a arquitetura do cluster

Antes de mexer em qualquer dado, vale entender **quem é quem**:

```bash
docker ps --format "table {{.Names}}\t{{.Image}}\t{{.Status}}"
```

Você deve ver 7 containers (ou mais, se acrescentar datanodes/workers extras ao longo do laboratório):

| Container | Papel | O que acontece se ele cair |
|---|---|---|
| `namenode` | Guarda os **metadados** do HDFS (onde cada arquivo está, em que blocos, em quais datanodes) | Sem ele, ninguém sabe onde os dados estão — mesmo que os dados físicos ainda existam nos datanodes |
| `datanode` | Guarda os **blocos de dados** de verdade, fisicamente | Os arquivos que só tinham cópia nele ficam inacessíveis (por isso a replicação existe — Parte 3) |
| `resourcemanager` | Decide quais recursos (CPU/memória) cada aplicação pode usar no cluster (parte do YARN) | Aplicações novas não conseguem ser agendadas |
| `nodemanager` | Executa as tarefas designadas pelo `resourcemanager` numa máquina específica | Essa máquina para de processar tarefas do YARN |
| `historyserver` | Guarda o histórico de jobs do Hadoop/YARN já terminados | Perde-se a visibilidade de jobs antigos, mas jobs novos continuam funcionando |
| `spark-master` | Coordena o cluster **Spark** (gerenciador de recursos independente do YARN — usado diretamente neste laboratório) | Nenhum job Spark novo consegue rodar |
| `spark-worker-1` (e `-2`) | Executa as tarefas Spark de verdade | Perde-se capacidade de processamento, mas o cluster continua funcionando com os workers restantes |

**Pergunta para discussão:** por que existem *dois* sistemas de gerenciamento de recursos aparentemente redundantes (`resourcemanager`/YARN do Hadoop e `spark-master` do Spark)? *(O Spark pode rodar tanto em seu próprio gerenciador standalone — o que fazemos aqui — quanto usando o YARN como gerenciador compartilhado entre várias ferramentas do ecossistema Hadoop. Usamos o modo standalone por simplicidade; em produção, é comum várias ferramentas compartilharem o YARN.)*

**Confirme que todos os containers estão na mesma rede:**

```bash
docker network inspect hadoop_spark_net --format '{{range .Containers}}{{.Name}} -> {{.IPv4Address}}{{"\n"}}{{end}}'
```

---

## Parte 3 — HDFS: blocos, réplicas e tolerância a falhas

O HDFS (*Hadoop Distributed File System*) não guarda arquivos como um sistema de arquivos comum — ele quebra cada arquivo em **blocos** (128 MB por padrão) e distribui esses blocos entre os datanodes, geralmente com **réplicas** (várias cópias do mesmo bloco em máquinas diferentes), para tolerar a queda de um nó sem perder dados.

### 3.1 — Estado inicial: 1 datanode

```bash
docker exec namenode hdfs dfsadmin -report | grep -A5 "Live datanodes"
```

Nesse ponto deve aparecer **1 datanode vivo** — o único que sobe com o cluster inicial.

### 3.2 — Gravando um arquivo pequeno

```bash
cat > pratica_hdfs.txt << 'EOF'
Este é um arquivo de teste para praticar o envio de dados ao HDFS.
EOF

docker exec namenode hdfs dfs -mkdir -p /pratica
docker cp pratica_hdfs.txt namenode:/tmp/
docker exec namenode hdfs dfs -put /tmp/pratica_hdfs.txt /pratica/
```

### 3.3 — Verificando os blocos

```bash
docker exec namenode hdfs fsck /pratica/pratica_hdfs.txt -files -blocks -locations
```

**Esperado:** 1 bloco só (arquivo pequeno), `Live_repl=1` — só existe 1 datanode pra guardar qualquer réplica, mesmo que o fator de replicação padrão configurado seja maior (geralmente 3). O `Status` continua `HEALTHY` mesmo assim: sub-replicado não é o mesmo que corrompido.

**Leia o conteúdo de volta, direto do HDFS** (sem baixar pro Codespace):

```bash
docker exec namenode hdfs dfs -cat /pratica/pratica_hdfs.txt
```

> Vale o hábito de testar a leitura a cada etapa daqui em diante — é exatamente esse comando que vamos usar na Parte 3.14 para confirmar se um arquivo continua acessível depois de derrubarmos um datanode de propósito.

### 3.4 — Acrescentando o segundo datanode

```bash
cat >> docker-hadoop/docker-compose.override.yml << 'EOF'
services:
  datanode-2:
    image: bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8
    container_name: datanode-2
    restart: always
    volumes:
      - hadoop_datanode2:/hadoop/dfs/data
    environment:
      SERVICE_PRECONDITION: "namenode:9870"
    env_file:
      - ./hadoop.env

volumes:
  hadoop_datanode2:
EOF

cd docker-hadoop
docker-compose config > /dev/null && echo "YAML válido" || echo "ERRO no YAML"
docker-compose up -d
cd ..

docker exec namenode hdfs dfsadmin -report | grep "Live datanodes"
```

### 3.5 — Replicando manualmente o arquivo pequeno

```bash
docker exec namenode hdfs dfs -setrep -w 2 /pratica/pratica_hdfs.txt
docker exec namenode hdfs fsck /pratica/pratica_hdfs.txt -files -blocks -locations
```

**Esperado:** `Live_repl=2`. Precisou do `-setrep` manual porque o `pratica_hdfs.txt` já existia antes do `datanode-2` subir — o HDFS não replica retroativamente sozinho. Guarde essa observação: no próximo passo, um arquivo **novo** (enviado depois deste ponto) vai se comportar de forma bem diferente.

### 3.6 — Incluindo um arquivo grande (400 MB)

Agora, com 2 datanodes já de pé, envie um arquivo novo:

```bash
dd if=/dev/urandom of=arquivo_teste_400mb.bin bs=1M count=400 status=progress

docker cp arquivo_teste_400mb.bin namenode:/tmp/
docker exec namenode hdfs dfs -put /tmp/arquivo_teste_400mb.bin /pratica/
```

### 3.7 — Verificando os blocos do arquivo grande

```bash
docker exec namenode hdfs fsck /pratica/arquivo_teste_400mb.bin -files -blocks -locations
```

**Esperado:** com bloco padrão de 128 MB, um arquivo de 400 MB vira **4 blocos** (3 de 128 MB + 1 de 16 MB) — e, diferente do `pratica_hdfs.txt` no passo 3.5, esses blocos já devem vir com `Live_repl=2` **automaticamente**, sem precisar de nenhum `-setrep`. É a diferença entre um arquivo escrito quando só havia 1 datanode disponível, e um arquivo escrito depois que a capacidade do cluster já tinha aumentado.

### 3.8 (extra) — Alterando o tamanho do bloco

O tamanho de bloco também é configurável — não é uma constante fixa do HDFS.

**Só para um arquivo**, via `-D` no próprio comando:

```bash
docker exec namenode hdfs dfs -D dfs.blocksize=67108864 -put /tmp/arquivo_teste_400mb.bin /pratica/arquivo_bloco_64mb.bin
docker exec namenode hdfs fsck /pratica/arquivo_bloco_64mb.bin -files -blocks -locations
```

Com 64 MB de bloco (`67108864` bytes), o mesmo arquivo de 400 MB deve virar **7 blocos** (`ceil(400/64) = 7`).

> **Sobre o aviso `Under replicated` no `fsck`:** nesse ponto do roteiro você tem 2 datanodes (passo 3.4), mas o fator de replicação padrão ainda é 3 — o `fsck` lista cada bloco individualmente avisando que só encontrou 2 réplicas em vez de 3 (`Target Replicas is 3 but found 2 live replica(s)`). Isso é esperado, é a mesma situação do passo 3.7, só que aqui o `fsck` sem `-locations` é mais verboso nesse aviso. `Status: HEALTHY` no final confirma que não há perda de dado, só uma redundância abaixo do ideal — que será corrigida quando o `datanode-3` entrar, no passo 3.9.

**Como padrão do cluster** (afeta só arquivos enviados depois):

```bash
echo "HDFS_CONF_dfs_blocksize=67108864" >> docker-hadoop/hadoop.env
cd docker-hadoop && docker-compose up -d && cd ..
docker exec namenode hdfs getconf -confKey dfs.blocksize
```

**Consultar o tamanho de bloco configurado, a qualquer momento:**

```bash
docker exec namenode hdfs getconf -confKey dfs.blocksize
```

**Listar os arquivos armazenados em uma pasta:**

```bash
docker exec namenode hdfs dfs -ls -h /pratica
```

**Confirmando que o conteúdo não muda, só a forma como é fragmentado:**

Como `arquivo_teste_400mb.bin` e `arquivo_bloco_64mb.bin` vêm da mesma origem, dá pra provar que o tamanho de bloco é só um detalhe de armazenamento físico, sem nenhum efeito sobre o conteúdo lógico:

```bash
# Espiar os primeiros bytes de cada um (conteúdo é binário aleatório -- olhar em hexadecimal)
docker exec namenode hdfs dfs -cat /pratica/arquivo_teste_400mb.bin | head -c 64 | xxd
docker exec namenode hdfs dfs -cat /pratica/arquivo_bloco_64mb.bin | head -c 64 | xxd

# Comparar o hash do conteúdo inteiro
md5sum arquivo_teste_400mb.bin
docker exec namenode hdfs dfs -cat /pratica/arquivo_teste_400mb.bin | md5sum
docker exec namenode hdfs dfs -cat /pratica/arquivo_bloco_64mb.bin | md5sum
```

Os três `md5sum` devem bater exatamente, e os 64 bytes iniciais devem ser idênticos entre os dois arquivos.

> **Evite `hdfs dfs -checksum` para essa comparação** — o checksum nativo do HDFS é calculado hierarquicamente, por bloco, então dois arquivos com conteúdo idêntico mas tamanhos de bloco diferentes podem gerar checksums HDFS diferentes. O `md5sum` sobre o fluxo de bytes (via `-cat`) hasheia o conteúdo lógico, não a forma como foi fragmentado — é a comparação correta aqui.

### 3.9 — Acrescentando o terceiro datanode

```bash
cat >> docker-hadoop/docker-compose.override.yml << 'EOF'
  datanode-3:
    image: bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8
    container_name: datanode-3
    restart: always
    volumes:
      - hadoop_datanode3:/hadoop/dfs/data
    environment:
      SERVICE_PRECONDITION: "namenode:9870"
    env_file:
      - ./hadoop.env
EOF
```

> **Atenção:** o `datanode-3` precisa entrar **dentro** do bloco `services:` já existente, e `hadoop_datanode3:` dentro do bloco `volumes:` já existente — não duplique essas chaves no YAML.

```bash
cd docker-hadoop
docker-compose config > /dev/null && echo "YAML válido" || echo "ERRO no YAML"
docker-compose up -d
cd ..
```

### 3.10 — Verificando a replicação plena (fator 3)

```bash
docker exec namenode hdfs dfs -setrep -w 3 /pratica/arquivo_teste_400mb.bin
docker exec namenode hdfs fsck /pratica/arquivo_teste_400mb.bin -files -blocks -locations
```

**Esperado:** `Live_repl=3` em todos os blocos, `Under-replicated blocks: 0`. Com 3 nós e fator 3, só existe uma combinação possível de distribuição — todo bloco precisa estar nos 3 nós ao mesmo tempo. Repare que precisou de `-setrep` de novo aqui: o arquivo tinha nascido com 2 réplicas (passo 3.7, quando só havia 2 datanodes), e o terceiro nó não corrige isso sozinho — mesma lógica do passo 3.5.

### 3.11 — Alterando o fator de replicação para 2

```bash
echo "HDFS_CONF_dfs_replication=2" >> docker-hadoop/hadoop.env
cd docker-hadoop && docker-compose up -d && cd ..
docker exec namenode hdfs getconf -confKey dfs.replication
```

> **Atenção:** use `docker-compose up -d`, nunca `docker-compose restart` — o `restart` reinicia o container existente sem reler o `hadoop.env`, então a mudança não teria efeito.

### 3.12 — Verificando a distribuição dos blocos com réplica = 2

```bash
docker exec namenode hdfs dfs -put /tmp/arquivo_teste_400mb.bin /pratica/arquivo_teste_400mb_v2.bin
docker exec namenode hdfs fsck /pratica/arquivo_teste_400mb_v2.bin -files -blocks -locations
```

**Esperado:** `Live_repl=2`, mas agora, com 3 nós disponíveis e só 2 réplicas necessárias, cada bloco tem liberdade de escolher **quais** 2 dos 3 nós usar — vale comparar as combinações de datanodes bloco a bloco.

### 3.13 — Acelerando a detecção de falhas (para viabilizar o teste em aula)

```bash
echo "HDFS_CONF_dfs_namenode_heartbeat_recheck___interval=10000" >> docker-hadoop/hadoop.env
cd docker-hadoop
docker-compose up -d --force-recreate namenode
cd ..

docker exec namenode grep -A1 "recheck" /opt/hadoop-3.2.1/etc/hadoop/hdfs-site.xml
```

> **Atenção com a sintaxe:** a propriedade real (`dfs.namenode.heartbeat.recheck-interval`) tem um **hífen**. Nas variáveis de ambiente dessa imagem, hífen não se escreve como hífen — usa-se **três underscores**. Um hífen literal falha silenciosamente (a variável aparece no `printenv`, mas nunca chega no XML de configuração). Convenção completa:
>
> | Na variável de ambiente | Vira, na propriedade |
> |---|---|
> | `_` | `.` |
> | `__` | `_` |
> | `___` | `-` |
>
> Com `recheck-interval=10000` (10s) e `heartbeat-interval` padrão (3s), a fórmula `2×recheck + 10×heartbeat` dá **~50 segundos** até um nó parado ser oficialmente declarado morto (contra ~10,5 minutos do padrão de fábrica).

### 3.14 — Testando a tolerância a falhas

```bash
# 1. Leitura de linha de base
docker exec namenode hdfs dfs -cat /pratica/arquivo_teste_400mb_v2.bin > /dev/null && echo "Leitura OK"

# 2. Identifique o container em cada IP (com base no fsck do passo 3.12)
docker network inspect hadoop_spark_net --format '{{range .Containers}}{{.Name}} -> {{.IPv4Address}}{{"\n"}}{{end}}'

# 3. Derrube um datanode que guarda réplica do arquivo
date +%T && docker stop <nome-do-datanode-escolhido>

# 4. Teste a leitura imediatamente -- deve continuar funcionando
docker exec namenode hdfs dfs -cat /pratica/arquivo_teste_400mb_v2.bin > /dev/null && echo "Ainda acessível, mesmo com um nó fora do ar!"

# 5. Espere ~50s e confirme a declaração oficial de morte
sleep 50
date +%T && docker exec namenode hdfs dfsadmin -report | grep -A2 "Live datanodes\|Dead datanodes"

# 6. Confirme a autocorreção da replicação nos nós restantes
docker exec namenode hdfs fsck /pratica/arquivo_teste_400mb_v2.bin -files -blocks -locations

# 7. Recuperação
docker start <nome-do-datanode-escolhido>
docker exec namenode hdfs dfsadmin -report | grep "Live datanodes"
```

**Pergunta para ir além:** existe um limite pra essa tolerância. Se você derrubasse **2** dos 3 datanodes ao mesmo tempo (com fator de replicação 2), a leitura ainda funcionaria? E se fosse exatamente o nó que guarda a *única* cópia restante de algum bloco? O que isso ensina sobre a relação entre fator de replicação e quantas falhas simultâneas um cluster realmente tolera?

---

## Parte 4 — Uma nota sobre o YARN

Vale abrir a porta **8088** (ResourceManager) uma vez, só para reconhecimento — mas repare que ela provavelmente está **vazia** (sem aplicações listadas). Isso é esperado: o job da Parte 6 roda no **Spark Standalone** (`spark://spark-master:7077`), não através do YARN. O YARN está de pé como parte do ecossistema Hadoop, mas não é usado para agendar as tarefas Spark neste laboratório.

```bash
docker exec resourcemanager yarn node -list
```

Esse comando deve listar o(s) `nodemanager`(s) registrados — confirma que o YARN está de pé e funcional, ainda que não seja o gerenciador usado pelos jobs Spark aqui.

---

## Parte 5 — Explorando o cluster Spark

### Preparando a base de dados

Os testes desta parte e da Parte 6 usam uma amostra de 1 milhão de linhas de corridas de táxi de Nova York (2024), hospedada no Hugging Face. Baixe direto no Codespace, via `wget`, e envie ao HDFS:

```bash
wget https://huggingface.co/datasets/alexvaroz/nyc_taxi_trip_2024_p1_sample/resolve/main/nyc_tripdata_2024_sample_1M.csv

docker cp nyc_tripdata_2024_sample_1M.csv namenode:/tmp/
docker exec namenode hdfs dfs -mkdir -p /dados
docker exec namenode hdfs dfs -put -f /tmp/nyc_tripdata_2024_sample_1M.csv /dados/
```

> Com 1 milhão de linhas, esse arquivo é bem maior que os exemplos sintéticos da Parte 3 — vale reaproveitar os comandos de lá (`hdfs dfs -ls -h /dados`, `hdfs fsck ... -files -blocks -locations`) pra ver quantos blocos essa base real ocupa e como ficou distribuída entre os datanodes que você já tiver configurado nesse ponto do laboratório.

### Pela interface web

- **Porta 8080** (Spark Master): mostra quantos workers estão conectados, quantos cores/memória cada um tem, e a lista de aplicações rodando ou já concluídas.
- **Porta 8081** (Worker 1) e **8082** (Worker 2): detalhes de execução daquele worker especificamente.

### Pela linha de comando

```bash
docker exec spark-master curl -s http://localhost:8080 | grep -o "Alive Workers: [0-9]*"
docker logs spark-master --tail 30
docker logs spark-worker-1 --tail 30
```

### Reconhecendo o cluster parado

Antes de rodar qualquer job, abra a porta 8080 e registre: quantos *Workers* aparecem, e quantos *cores*/memória cada um anuncia. A soma dos cores é o paralelismo máximo teórico do cluster nesse momento.

### O particionamento e o shuffle, ao vivo

Use o shell interativo do PySpark para rodar comando a comando enquanto observa a porta 4040 (Spark Application UI, só existe enquanto um job está rodando — veja a nota sobre como publicar essa porta mais abaixo, se ela ainda não estiver acessível):

```bash
docker exec -it spark-master /spark/bin/pyspark --master spark://spark-master:7077
```

```python
df = spark.read.csv("hdfs://namenode:9000/dados/nyc_tripdata_2024_sample_1M.csv", header=True)
df.rdd.getNumPartitions()

# Sem shuffle -- cada partição resolve sozinha
df.filter(df.payment_type == "1").count()

# Com shuffle -- precisa redistribuir dados entre partições por chave
df.groupBy("payment_type").count().show()
```

Compare as duas últimas operações na aba **Stages** da porta 4040: o `filter()` gera 1 stage; o `groupBy()` gera 2, com uma quebra no meio (o shuffle) e métricas de **Shuffle Read/Write**. Essa quebra de stage é, conceitualmente, o mesmo papel que a fase de *shuffle* cumpre no paradigma MapReduce — só que aqui, distribuída de verdade entre processos/máquinas.

> **Publicando a porta 4040, se necessário:** essa porta costuma não vir publicada por padrão no `docker-compose.yml` do `docker-spark`. Confira com `docker port spark-master`; se não aparecer, adicione ao `docker-spark/docker-compose.override.yml` (repetindo as portas já existentes do `spark-master`, para não substituí-las):
> ```yaml
> services:
>   spark-master:
>     ports:
>       - "7077:7077"
>       - "8080:8080"
>       - "4040:4040"
> ```
> Depois, `docker-compose up -d` na pasta `docker-spark`.

### Tolerância a falhas no Spark: matando um worker no meio da execução

```python
import time
inicio = time.time()
df.join(df, "payment_type").count()
print("Tempo:", time.time() - inicio)
```

Enquanto esse comando roda, em outro terminal:

```bash
docker stop <nome-do-worker>
```

Observe na porta 4040 (ou nos logs do driver) se o Spark reatribui as tasks perdidas para outro worker, ou se o job falha (dependendo de quantos workers restam). Depois, suba o worker de novo: `docker start <nome-do-worker>`.

---

## Parte 6 — Executando um job PySpark real no cluster

Este é o fechamento prático: pegar um script PySpark, enviá-lo ao cluster e observar a execução usando os mesmos dados já preparados na Parte 5 (`/dados/nyc_tripdata_2024_sample_1M.csv` no HDFS).

### 6.1 — O script (`payment_type.py`)

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("PaymentTypeAnalysis") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

df = spark.read.csv("hdfs://namenode:9000/dados/nyc_tripdata_2024_sample_1M.csv", header=True)

# Corridas por tipo de pagamento
df.groupBy("payment_type").count().orderBy("count", ascending=False).show()

# Receita total por tipo de pagamento
df.groupBy("payment_type").sum("total_amount").show()

# Tarifa média por tipo de pagamento
df.groupBy("payment_type").avg("fare_amount").show()

spark.stop()
```

### 6.2 — Enviando o script ao cluster

```bash
docker cp payment_type.py spark-master:/payment_type.py
```

> Se estiver rodando esta parte numa sessão nova, sem ter passado pela Parte 5 antes, repita a preparação dos dados de lá (`wget` + `docker cp` + `hdfs dfs -put`) antes de continuar.

### 6.3 — Executando, observando ao vivo

Deixe a porta **8080** aberta numa aba antes de rodar:

```bash
docker exec -it spark-master /spark/bin/spark-submit --master spark://spark-master:7077 /payment_type.py
```

Enquanto o job roda, atualize a porta 8080 — a aplicação deve aparecer em **Running Applications**, com o `Application ID` e os recursos alocados; ao terminar, migra para **Completed Applications**. A saída do `spark-submit` mostra as três tabelas de resultado direto no console.

---

## Parte 7 — Lições e problemas comuns

Estas descobertas foram feitas ao longo do desenvolvimento deste laboratório e já estão incorporadas ao `setup.sh` sempre que possível — mas vale conhecê-las, porque ilustram problemas reais de operação de sistemas distribuídos:

| Descoberta | Resumo |
|---|---|
| Configuração não aplica com `restart` | `docker-compose restart` reinicia o container existente sem reler o `env_file` — mudanças em `hadoop.env` exigem `docker-compose up -d` |
| Hífen em propriedade precisa de `___` | Nome de variável de ambiente com hífen literal falha silenciosamente; use a convenção `_`→`.`, `__`→`_`, `___`→`-` |
| Hostname interno não resolvível | Containers podem se registrar com um hostname Docker bruto, não resolvível pela rede — corrigido com `dfs.client.use.datanode.hostname=false` e `dfs.datanode.use.datanode.hostname=false` (já incorporado ao `setup.sh`) |
| Detecção de falha não é instantânea | Existe uma janela real entre "o nó caiu" e "o namenode sabe que caiu" (~10,5 min por padrão) — nessa janela, o comportamento pode ser inconsistente |
| Réplica automática depende de capacidade | Um arquivo novo nasce com o máximo de réplicas *possível* dado o nº de nós disponíveis — arquivos antigos só se ajustam com `-setrep` manual ou, se mais nós aparecerem depois, por autocorreção em segundo plano |
| Tamanho de bloco é configurável | Assim como réplica e timeout, não é uma constante fixa do HDFS |
| Redes isoladas por projeto | Dois `docker-compose.yml` em pastas diferentes criam redes Docker separadas por padrão — resolvido com uma rede externa compartilhada, criada antes de qualquer `docker-compose up` |
| Duas tabelas de `iptables` coexistindo | Ambientes Docker-in-Docker podem ter `iptables-nft` e `iptables-legacy` simultaneamente, com políticas diferentes — o `setup.sh` verifica e corrige ambas |

---

## Encerrando o ambiente

**Pausar os containers, preservando tudo** (mais rápido para retomar depois):

```bash
cd docker-hadoop && docker-compose stop && cd ..
cd docker-spark && docker-compose stop && cd ..
```

**Derrubar containers e rede, preservando os dados do HDFS** (volumes nomeados sobrevivem):

```bash
cd docker-hadoop && docker-compose down && cd ..
cd docker-spark && docker-compose down && cd ..
```

**Resetar tudo, incluindo os dados** (começar 100% do zero):

```bash
cd docker-hadoop && docker-compose down -v && cd ..
cd docker-spark && docker-compose down -v && cd ..
```

Para retomar depois de qualquer uma das opções acima, rode `./setup.sh` novamente — ele é idempotente e reconstrói exatamente o que faltar.

---

## Síntese para o relatório

1. Desenhe a arquitetura completa dos containers e as setas de comunicação entre eles, com base no que foi observado neste laboratório.
2. O que aconteceria com os dados armazenados em `/pratica` se **um** dos containers `datanode` fosse destruído agora? A resposta muda dependendo de qual arquivo você escolher (um replicado manualmente com `-setrep`, ou um replicado automaticamente desde o início)? E se fosse o `namenode` que caísse?
3. Por que o ResourceManager (YARN) aparece vazio mesmo com o cluster funcionando perfeitamente? O que isso ensina sobre a diferença entre "o Hadoop está rodando" e "estou usando todos os componentes do Hadoop"?
4. Relacione a fase de *shuffle* observada na Parte 5 (via `.explain()` ou na UI do Spark) com a implementação manual do paradigma MapReduce feita anteriormente na disciplina. O que muda entre fazer isso em um `dict` Python local e fazer isso entre processos/máquinas separadas?
5. O `payment_type.py` rodou sobre uma base real de 1 milhão de linhas. Anote o tempo total de execução (visível no fim da saída do `spark-submit` ou na coluna `Duration` da porta 8080) e repita o teste limitando o cluster a menos recursos (por exemplo, com só 1 worker Spark ativo). O tempo aumenta na proporção esperada? O que isso sugere sobre os limites do cluster montado (poucos cores, pouca memória) neste ambiente de Codespaces?

---

## Referências

- Big Data Europe (BDE) — projeto de pesquisa financiado pelo programa Horizon 2020 da União Europeia (2015–2017), origem das imagens Docker `bde2020/hadoop-*` e `bde2020/spark-*` usadas neste laboratório.
- Documentação oficial do [Apache Hadoop](https://hadoop.apache.org/docs/stable/) e do [Apache Spark](https://spark.apache.org/docs/latest/).
- [GitHub Codespaces](https://docs.github.com/codespaces) — documentação oficial.
