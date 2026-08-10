import { onCall, HttpsError } from "firebase-functions/v2/https";
import { onSchedule } from "firebase-functions/v2/scheduler";
import { onDocumentWritten } from "firebase-functions/v2/firestore";
import { defineSecret } from "firebase-functions/params";
import { initializeApp } from "firebase-admin/app";
import { getFirestore } from "firebase-admin/firestore";
import { getMessaging } from "firebase-admin/messaging";

const MAXIPROD_TOKEN = defineSecret("MAXIPROD_TOKEN");
const MAXIPROD_GRAPHQL_URL = "https://api.maxiprod.com.br/graphql/";
const MAX_DEPTH = 12;
const COST_STRUCTURE_CHUNK_SIZE = 600000;
const SHARED_MAXIPROD_CACHE_ID = "maxiprod-shared-cache";
// 200 mil caracteres permanecem abaixo do limite de 1 MiB mesmo quando o JSON
// contém muitos caracteres UTF-8 de até quatro bytes.
const SHARED_MAXIPROD_CHUNK_SIZE = 200000;
const SHARED_MAXIPROD_WRITE_CONCURRENCY = 8;
const SHARED_MAXIPROD_WRITE_RETRIES = 4;

initializeApp();

function costStructureBaseId(orderId) {
  return `cost-structure-${String(orderId).replace(/[^A-Za-z0-9_-]/g, "_")}`;
}

async function saveCostStructureDocument(database, orderId, itemCode, structure) {
  const baseId = costStructureBaseId(orderId);
  const collection = database.collection("cartamac");
  const manifestRef = collection.doc(baseId);
  const json = JSON.stringify(Array.isArray(structure) ? structure : []);
  const chunks = [];
  for (let start = 0; start < json.length; start += COST_STRUCTURE_CHUNK_SIZE) {
    chunks.push(json.slice(start, start + COST_STRUCTURE_CHUNK_SIZE));
  }
  if (!chunks.length) chunks.push("[]");

  let previousChunks = 0;
  try {
    const previous = await manifestRef.get();
    previousChunks = Number(previous.data()?.chunkCount) || 0;
  } catch {
    previousChunks = 0;
  }

  await Promise.all(chunks.map((content, index) => collection.doc(`${baseId}-chunk-${index}`).set({
    content,
    index,
    updatedAt: new Date().toISOString(),
  })));
  await manifestRef.set({
    format: "json-chunks-v1",
    itemCode: String(itemCode || ""),
    chunkCount: chunks.length,
    characterCount: json.length,
    updatedAt: new Date().toISOString(),
  });

  if (previousChunks > chunks.length) {
    await Promise.all(Array.from(
      { length: previousChunks - chunks.length },
      (_, offset) => collection.doc(`${baseId}-chunk-${chunks.length + offset}`).delete().catch(() => {}),
    ));
  }
  return { docId: baseId, chunkCount: chunks.length, characterCount: json.length };
}

function wait(milliseconds) {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

async function setSharedCacheDocumentWithRetry(reference, data, label) {
  let lastError = null;
  for (let attempt = 1; attempt <= SHARED_MAXIPROD_WRITE_RETRIES; attempt++) {
    try {
      await reference.set(data);
      return;
    } catch (error) {
      lastError = error;
      console.warn(`Cache compartilhado: falha ao gravar ${label} (tentativa ${attempt}).`, {
        code: error?.code || null,
        message: error?.message || String(error),
      });
      if (attempt < SHARED_MAXIPROD_WRITE_RETRIES) {
        await wait(500 * (2 ** (attempt - 1)));
      }
    }
  }
  throw lastError;
}

async function saveSharedMaxiprodCache(database, payload) {
  const collection = database.collection("cartamac");
  const manifestRef = collection.doc(SHARED_MAXIPROD_CACHE_ID);
  const json = JSON.stringify(payload || {});
  const chunks = [];
  for (let start = 0; start < json.length; start += SHARED_MAXIPROD_CHUNK_SIZE) {
    chunks.push(json.slice(start, start + SHARED_MAXIPROD_CHUNK_SIZE));
  }
  if (!chunks.length) chunks.push("{}");

  let previousChunks = 0;
  let previousChunkBaseId = SHARED_MAXIPROD_CACHE_ID;
  try {
    const previous = await manifestRef.get();
    previousChunks = Number(previous.data()?.chunkCount) || 0;
    previousChunkBaseId = String(previous.data()?.chunkBaseId || SHARED_MAXIPROD_CACHE_ID);
  } catch {
    previousChunks = 0;
  }

  const updatedAt = String(payload?.updatedAt || new Date().toISOString());
  // Cada atualizacao recebe nomes exclusivos. O manifesto so aponta para essa
  // versao depois que todos os blocos estiverem salvos, evitando que duas
  // execucoes simultaneas misturem partes de fotografias diferentes.
  const generationId = `${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
  const chunkBaseId = `${SHARED_MAXIPROD_CACHE_ID}-${generationId}`;
  for (let start = 0; start < chunks.length; start += SHARED_MAXIPROD_WRITE_CONCURRENCY) {
    const group = chunks.slice(start, start + SHARED_MAXIPROD_WRITE_CONCURRENCY);
    await Promise.all(group.map((content, offset) => {
      const index = start + offset;
      return setSharedCacheDocumentWithRetry(
        collection.doc(`${chunkBaseId}-chunk-${index}`),
        { content, index, updatedAt, generationId },
        `bloco ${index + 1} de ${chunks.length}`,
      );
    }));
  }
  const manifest = {
    format: "json-chunks-v1",
    version: Number(payload?.version) || 1,
    generationId,
    chunkBaseId,
    chunkCount: chunks.length,
    characterCount: json.length,
    currentYear: Number(payload?.currentYear) || new Date().getFullYear(),
    updatedAt,
    stats: payload?.stats || {},
  };
  await setSharedCacheDocumentWithRetry(manifestRef, manifest, "manifesto");

  // A limpeza ocorre somente depois da publicacao e nunca invalida a versao
  // nova se o Firestore estiver momentaneamente indisponivel.
  if (previousChunks && previousChunkBaseId !== chunkBaseId) {
    for (let start = 0; start < previousChunks; start += SHARED_MAXIPROD_WRITE_CONCURRENCY) {
      const count = Math.min(SHARED_MAXIPROD_WRITE_CONCURRENCY, previousChunks - start);
      await Promise.all(Array.from({ length: count }, (_, offset) =>
        collection.doc(`${previousChunkBaseId}-chunk-${start + offset}`).delete().catch((error) => {
          console.warn("Cache compartilhado: nao foi possivel remover um bloco antigo.", {
            code: error?.code || null,
            message: error?.message || String(error),
          });
        })));
    }
  }
  return manifest;
}

function numberOrNull(value) {
  if (value === null || value === undefined || value === "") return null;
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function safeItemCode(value) {
  const code = String(value || "").trim();
  if (!code || code.length > 60 || !/^[A-Za-z0-9._+\-]+$/.test(code)) {
    throw new HttpsError("invalid-argument", "Informe um código MaxiProd válido.");
  }
  return code;
}

function categoryForItem(code, description) {
  const normalizedCode = String(code || "").trim().toUpperCase();
  const normalizedDescription = String(description || "").toUpperCase();
  if (normalizedCode.startsWith("MP")) return "Matéria-prima";
  if (/USINAGEM|TEMPERA|TRATAMENTO|PINTURA|CALDEIRARIA|SOLDA|CORTE|SERVIÇO/.test(normalizedDescription)) return "Serviço";
  if (normalizedCode.startsWith("DS")) return "Conjunto fabricado";
  return "Comercial";
}

async function maxiprodQuery(query, variables, token) {
  const response = await fetch(MAXIPROD_GRAPHQL_URL, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Basic ${token}`,
    },
    body: JSON.stringify({ query, variables }),
  });

  if (!response.ok) {
    const body = (await response.clone().text()).slice(0, 1200);
    console.error("MaxiProd HTTP error", { status: response.status, statusText: response.statusText, body });
    throw new HttpsError("unavailable", `MaxiProd HTTP ${response.status}: ${body || response.statusText}`);
  }

  const payload = await response.json();
  if (payload.errors?.length) {
    const messages = payload.errors.map((error) => error.message || "Erro GraphQL sem descrição").join(" | ").slice(0, 1200);
    console.error("MaxiProd GraphQL error", messages);
    throw new HttpsError("failed-precondition", `MaxiProd GraphQL: ${messages}`);
  }
  return payload.data;
}

const STRUCTURE_QUERY = `
  query Estrutura($codigo: String!) {
    itensOuGruposDaEstruturaDoProduto(
      take: 500
      skip: 0
      where: { itemOuGrupoPai: { codigo: { eq: $codigo } } }
    ) {
      items {
        ordem
        quantidadeBruta
        quantidadeLiquida
        perdaEmPercentual
        itemOuGrupoFilho {
          codigo
          descricao
          unidade { codigo }
          precoDeCompraUltimoValor
          precoDeCompraUltimaAtualizacao
          ultimaNotaFiscalRecebidaDoItem {
            numero
            emissaoData
            quantidade
            valorUnitario
          }
        }
      }
    }
  }
`;

// Dados realizados da Ordem de Produção. O vínculo começa pelo código do item
// (ex.: 00671) porque é a informação disponível no Project/custos. A API pode
// devolver mais de uma OP para o mesmo item; a mais recentemente alterada é
// usada como referência e as demais são informadas no retorno.
const PRODUCTION_ORDERS_QUERY = `
  query OrdensDeProducao($codigo: String!) {
    ordensDeProducao(
      take: 100
      skip: 0
      where: { item: { codigo: { eq: $codigo } } }
    ) {
      items {
        id
        numero
        estado
        ultimaAlteracaoData
        item { codigo descricao unidade { codigo } }
      }
    }
  }
`;

// Mantemos estas consultas por compatibilidade com versões diferentes do
// MaxiProd. A consulta principal, porém, usa item.codigo, pois foi validada
// diretamente nesta base (00697 -> OP 7660).
const ITEM_BY_CODE_QUERY = `
  query ItemPorCodigo($codigo: String!) {
    itens(take: 10, skip: 0, where: { codigo: { eq: $codigo } }) {
      items { id codigo descricao }
    }
  }
`;

const PRODUCTION_ORDERS_BY_ITEM_ID_QUERY = `
  query OrdensDeProducaoPorItem($itemId: Long!) {
    ordensDeProducao(
      take: 100
      skip: 0
      where: { itemId: { eq: $itemId } }
    ) {
      items {
        id
        numero
        estado
        ultimaAlteracaoData
        item { codigo descricao unidade { codigo } }
      }
    }
  }
`;

// IMPORTANTE: nesta instalação os filtros abaixo, pelo número visível da OP,
// foram validados no Explorer GraphQL. Não usar ordemDeProducaoId aqui: em
// algumas versões ele é aceito pelo schema, mas retorna uma grade vazia.
const PRODUCTION_INSUMOS_BY_ORDER_QUERY = `
  query InsumosDaOPPorNumero($numero: Long!) {
    insumosDaOrdemDeProducao(
      take: 1000
      skip: 0
      where: { ordemDeProducao: { numero: { eq: $numero } } }
    ) {
      items {
        id
        ordem
        quantidadeLiquida
        quantidadeBaixada
        custoMedioUnitario
        item { codigo descricao unidade { codigo } precoDeCompraUltimoValor }
      }
    }
  }
`;

const PRODUCTION_OPERACOES_BY_ORDER_QUERY = `
  query OperacoesDaOPPorNumero($numero: Long!) {
    ordensDeProducaoOuManutencaoOperacoes(
      take: 1000
      skip: 0
      where: { ordemDeProducao: { numero: { eq: $numero } } }
    ) {
      items {
        id
        operacao
        descricao
        tempoRealizadoEmHoras
        custoRealizado
        estacaoDeTrabalho { codigo descricao }
      }
    }
  }
`;

// Em algumas OPs o consolidado tempoRealizadoEmHoras permanece em zero, mas
// existem apontamentos individuais. Esta consulta tenta trazer os horários
// para calcular a duração real. Se a versão do MaxiProd não expuser essa
// relação, a função volta automaticamente para a consulta básica acima.
const PRODUCTION_OPERACOES_WITH_APONTAMENTOS_QUERY = `
  query OperacoesDaOPComApontamentos($numero: Long!) {
    ordensDeProducaoOuManutencaoOperacoes(
      take: 1000
      skip: 0
      where: { ordemDeProducao: { numero: { eq: $numero } } }
    ) {
      items {
        id
        operacao
        descricao
        tempoRealizadoEmHoras
        custoRealizado
        estacaoDeTrabalho { codigo descricao }
        apontamentos { inicioData fimData }
      }
    }
  }
`;

// Algumas bases do MaxiProd não retornam insumos/operações dentro do objeto da
// OP, apesar de esses dados existirem nas consultas próprias. Esta consulta é
// usada como segunda tentativa para o mesmo código de item.
const PRODUCTION_RESOURCES_QUERY = `
  query RecursosDaOP($codigo: String!) {
    insumosDaOrdemDeProducao(
      take: 1000
      skip: 0
      where: { ordemDeProducao: { item: { codigo: { eq: $codigo } } } }
    ) {
      items {
        id
        ordem
        quantidadeLiquida
        quantidadeBaixada
        custoMedioUnitario
        ordemDeProducao { id numero estado item { codigo descricao unidade { codigo } } }
        item { codigo descricao unidade { codigo } precoDeCompraUltimoValor }
      }
    }
    ordensDeProducaoOuManutencaoOperacoes(
      take: 1000
      skip: 0
      where: { ordemDeProducao: { item: { codigo: { eq: $codigo } } } }
    ) {
      items {
        id
        ordem
        descricao
        tempoRealizadoEmHoras
        custoRealizado
        ordemDeProducao { id numero estado item { codigo descricao unidade { codigo } } }
        estacaoDeTrabalho { codigo descricao }
      }
    }
  }
`;

async function loadStructure(code, token) {
  const data = await maxiprodQuery(STRUCTURE_QUERY, { codigo: code }, token);
  return data?.itensOuGruposDaEstruturaDoProduto?.items || [];
}

function toNumber(value) {
  return numberOrNull(value) ?? 0;
}

function hoursFromApontamentos(apontamentos) {
  const rows = Array.isArray(apontamentos) ? apontamentos : (apontamentos?.items || []);
  return rows.reduce((total, apontamento) => {
    const inicio = new Date(apontamento?.inicioData || "").getTime();
    const fim = new Date(apontamento?.fimData || "").getTime();
    return Number.isFinite(inicio) && Number.isFinite(fim) && fim > inicio
      ? total + ((fim - inicio) / 3600000)
      : total;
  }, 0);
}

async function loadProductionOrder(itemCode, token) {
  // Esta é a mesma consulta que retornou OP 7660 para o item 00697 no
  // GraphQL Explorer. Evitamos a conversão desnecessária para itemId.
  const data = await maxiprodQuery(PRODUCTION_ORDERS_QUERY, { codigo: itemCode }, token);
  const orders = data?.ordensDeProducao?.items || [];
  if (!orders.length) {
    return {
      itemCode,
      ordemProducao: null,
      outrasOrdens: [],
      insumos: [],
      operacoes: [],
      atualizadoEm: new Date().toISOString(),
    };
  }

  const sorted = [...orders].sort((a, b) => String(b.ultimaAlteracaoData || "").localeCompare(String(a.ultimaAlteracaoData || "")));
  const order = sorted[0];
  // Não consultar as duas grades na mesma operação GraphQL: o MaxiProd executa
  // os resolvers em paralelo e pode acusar conflito interno de DbContext.
  // Insumos e operações são independentes. Se uma das grades não estiver
  // disponível nesta versão do MaxiProd, a outra continua sendo exibida.
  const avisos = [];
  let insumosData = null;
  let operacoesData = null;
  try {
    insumosData = await maxiprodQuery(PRODUCTION_INSUMOS_BY_ORDER_QUERY, { numero: order.numero }, token);
  } catch (error) {
    avisos.push(`Insumos: ${error?.message || "não foi possível consultar"}`);
  }
  try {
    try {
      operacoesData = await maxiprodQuery(PRODUCTION_OPERACOES_WITH_APONTAMENTOS_QUERY, { numero: order.numero }, token);
    } catch (apontamentosError) {
      // A grade de operações continua disponível mesmo nas versões que não
      // permitem abrir apontamentos como relação da operação.
      console.warn("MaxiProd: apontamentos não disponíveis; usando tempo consolidado", apontamentosError?.message || String(apontamentosError));
      operacoesData = await maxiprodQuery(PRODUCTION_OPERACOES_BY_ORDER_QUERY, { numero: order.numero }, token);
    }
  } catch (error) {
    avisos.push(`Operações: ${error?.message || "não foi possível consultar"}`);
  }
  const rawInsumos = insumosData?.insumosDaOrdemDeProducao?.items || [];
  const rawOperacoes = operacoesData?.ordensDeProducaoOuManutencaoOperacoes?.items || [];
  const insumos = rawInsumos.map((insumo) => {
    const quantidadeBaixada = toNumber(insumo.quantidadeBaixada);
    // O custo médio da baixa é a fonte principal. Quando não existir, o último
    // preço cadastrado no item é exibido como referência de estoque.
    const custoMedio = numberOrNull(insumo.custoMedioUnitario);
    const ultimoPreco = numberOrNull(insumo.item?.precoDeCompraUltimoValor);
    const valorUnitario = custoMedio ?? ultimoPreco ?? 0;
    return {
      id: insumo.id,
      ordem: insumo.ordem,
      codigo: insumo.item?.codigo || "",
      descricao: insumo.item?.descricao || "Insumo sem descrição",
      unidade: insumo.item?.unidade?.codigo || "",
      quantidadeBaixada,
      quantidadePrevista: toNumber(insumo.quantidadeLiquida),
      valorUnitario,
      origemDoValor: custoMedio !== null ? "custo médio da baixa" : ultimoPreco !== null ? "último preço do item" : "sem valor no MaxiProd",
      valorTotal: quantidadeBaixada * valorUnitario,
    };
  });

  const operacoes = rawOperacoes.map((operacao) => ({
    id: operacao.id,
    ordem: operacao.operacao,
    descricao: operacao.descricao || "Operação sem descrição",
    estacao: operacao.estacaoDeTrabalho?.descricao || operacao.estacaoDeTrabalho?.codigo || "Não definida",
    tempoRealizadoEmHoras: toNumber(operacao.tempoRealizadoEmHoras) || hoursFromApontamentos(operacao.apontamentos),
    fonteDoTempo: toNumber(operacao.tempoRealizadoEmHoras) > 0 ? "tempo consolidado da OP" : "apontamentos registrados",
    custoRealizado: numberOrNull(operacao.custoRealizado),
  }));

  return {
    itemCode,
    ordemProducao: {
      id: order.id,
      numero: order.numero,
      estado: order.estado || "",
      item: order.item?.codigo || itemCode,
      descricao: order.item?.descricao || "",
    },
    outrasOrdens: sorted.slice(1).map((other) => ({ id: other.id, numero: other.numero, estado: other.estado || "" })),
    insumos,
    operacoes,
    avisos,
    atualizadoEm: new Date().toISOString(),
  };
}

/**
 * Calcula a estrutura em cascata.
 * Itens com precoDeCompraUltimoValor usam o preço já normalizado pelo MaxiProd
 * para a unidade do item. Itens sem preço são abertos como subconjuntos.
 */
async function calculateStructureCost(rootCode, token) {
  const pricedItems = [];
  const missingCostItems = [];
  const activeCodes = new Set();

  async function walk(parentCode, factor, path, depth, parentUnitCost = null) {
    if (depth > MAX_DEPTH) {
      missingCostItems.push({
        codigo: parentCode,
        descricao: "Limite de níveis da estrutura atingido",
        quantidade: factor,
        motivo: "estrutura muito profunda",
        caminho: path,
      });
      return [];
    }

    if (activeCodes.has(parentCode)) {
      missingCostItems.push({
        codigo: parentCode,
        descricao: "Ciclo identificado na estrutura",
        quantidade: factor,
        motivo: "estrutura circular",
        caminho: path,
      });
      return [];
    }
    activeCodes.add(parentCode);

    const children = await loadStructure(parentCode, token);
    if (!children.length && depth > 0) {
      // Uma folha já pode ter seu próprio preço de compra. Só é pendência
      // quando o último preço ainda não existe ou está zerado (ex.: item
      // de serviço "A receber", sem NF recebida no MaxiProd).
      if (parentUnitCost === null || Number(parentUnitCost) === 0) {
        missingCostItems.push({
          codigo: parentCode,
          descricao: "Item sem preço disponível e sem estrutura cadastrada",
          quantidade: factor,
          motivo: "sem custo disponível",
          caminho: path,
        });
      }
      activeCodes.delete(parentCode);
      return [];
    }

    const treeNodes = [];
    for (const edge of children) {
      const child = edge.itemOuGrupoFilho;
      if (!child?.codigo) continue;

      const baseQuantity = numberOrNull(edge.quantidadeLiquida) ?? numberOrNull(edge.quantidadeBruta) ?? 0;
      const quantity = factor * baseQuantity;
      const unitCost = numberOrNull(child.precoDeCompraUltimoValor);
      const childPath = [...path, child.codigo];
      const node = {
        codigo: child.codigo,
        descricao: child.descricao || child.codigo,
        categoria: categoryForItem(child.codigo, child.descricao),
        unidade: child.unidade?.codigo || "",
        quantidade: quantity,
        quantidadeNoPai: baseQuantity,
        custoUnitario: unitCost,
        custoTotal: unitCost === null ? 0 : quantity * unitCost,
        perdaEmPercentual: numberOrNull(edge.perdaEmPercentual) ?? 0,
        ultimaAtualizacaoPreco: child.precoDeCompraUltimaAtualizacao || null,
        ultimaNotaFiscal: child.ultimaNotaFiscalRecebidaDoItem || null,
        caminho: childPath,
        filhos: [],
      };

      // O preço desta linha entra no cálculo, mas não encerra a busca.
      // Ex.: DS1783 > IN00934 > IN00933 > MP16932: todos os níveis com
      // preço precisam compor o total, como na estrutura multinível do MaxiProd.
      if (unitCost !== null) {
        pricedItems.push({
          ...node,
        });
      }
      node.filhos = await walk(child.codigo, quantity, childPath, depth + 1, unitCost);
      treeNodes.push(node);
    }
    activeCodes.delete(parentCode);
    return treeNodes;
  }

  const estrutura = await walk(rootCode, 1, [rootCode], 0);

  const custoTotal = pricedItems.reduce((sum, item) => sum + item.custoTotal, 0);
  return {
    codigoRaiz: rootCode,
    custoTotal,
    itensComCusto: pricedItems.length,
    itensSemCusto: missingCostItems.length,
    itens: pricedItems,
    pendencias: missingCostItems,
    estrutura,
    calculadoEm: new Date().toISOString(),
    criterio: "Último preço de compra normalizado pelo MaxiProd; estruturas sem custo direto são abertas em cascata.",
  };
}

// Monta exatamente o mesmo conjunto que o botão "Carregar estrutura" grava no
// aplicativo. A rotina também preserva os valores/hora definidos manualmente
// pela equipe para as operações.
async function refreshMaxiprodData(itemCode, previousMaxiprod, token) {
  // As chamadas precisam ser sequenciais: no MaxiProd consultas paralelas podem
  // concorrer pelo mesmo DbContext e resultar em erro interno.
  const structureResult = await calculateStructureCost(itemCode, token);
  const productionResult = await loadProductionOrder(itemCode, token).catch((error) => ({
      itemCode,
      ordemProducao: null,
      outrasOrdens: [],
      insumos: [],
      operacoes: [],
      avisos: [error?.message || "Não foi possível consultar a OP."],
      erro: error?.message || "Não foi possível consultar a OP.",
      atualizadoEm: new Date().toISOString(),
    }));

  const savedRates = previousMaxiprod?.production?.operationRates || {};
  return {
    itemCode,
    custoTotal: Number(structureResult.custoTotal) || 0,
    itensComCusto: Number(structureResult.itensComCusto) || 0,
    itensSemCusto: Number(structureResult.itensSemCusto) || 0,
    calculadoEm: structureResult.calculadoEm || new Date().toISOString(),
    criterio: structureResult.criterio || "",
    estrutura: Array.isArray(structureResult.estrutura) ? structureResult.estrutura : [],
    pendencias: (structureResult.pendencias || []).slice(0, 50).map((item) => ({
      codigo: item.codigo || "",
      descricao: item.descricao || "",
      motivo: item.motivo || "",
    })),
    production: {
      itemCode: productionResult.itemCode || itemCode,
      ordemProducao: productionResult.ordemProducao || null,
      outrasOrdens: Array.isArray(productionResult.outrasOrdens) ? productionResult.outrasOrdens : [],
      insumos: Array.isArray(productionResult.insumos) ? productionResult.insumos : [],
      operacoes: Array.isArray(productionResult.operacoes) ? productionResult.operacoes : [],
      avisos: Array.isArray(productionResult.avisos) ? productionResult.avisos : [],
      erro: productionResult.erro || "",
      atualizadoEm: productionResult.atualizadoEm || new Date().toISOString(),
      operationRates: savedRates,
    },
  };
}

// Atualização automática: executa a cada hora, apenas para OPs abertas que já
// tiveram um código MaxiProd informado. As OPs fechadas ficam congeladas.
export const atualizarCustosMaxiprodAutomaticamente = onSchedule(
  {
    schedule: "0 * * * *",
    timeZone: "America/Sao_Paulo",
    region: "southamerica-east1",
    // Funções agendadas aceitam no máximo 30 minutos no Cloud Functions.
    timeoutSeconds: 1800,
    memory: "1GiB",
    secrets: [MAXIPROD_TOKEN],
  },
  async () => {
    const database = getFirestore();
    const dbRef = database.collection("cartamac").doc("db");
    const snapshot = await dbRef.get();
    if (!snapshot.exists) {
      console.log("Atualização automática: documento cartamac/db não encontrado.");
      return;
    }

    const data = snapshot.data() || {};
    const costsData = data.costsData || {};
    const entries = Object.entries(costsData).filter(([, entry]) => {
      const code = String(entry?.maxiprod?.itemCode || "").trim();
      return code && !entry?.closedAt;
    });
    const summary = { startedAt: new Date().toISOString(), updated: 0, failed: 0, skipped: 0, errors: [] };

    // Sequencial para respeitar o limite da API do MaxiProd e evitar que duas
    // estruturas grandes concorram pelo mesmo contexto do ERP.
    for (const [orderId, entry] of entries) {
      const itemCode = String(entry.maxiprod.itemCode).trim().toUpperCase();
      try {
        const refreshed = await refreshMaxiprodData(itemCode, entry.maxiprod, MAXIPROD_TOKEN.value());
        const structure = Array.isArray(refreshed.estrutura) ? refreshed.estrutura : [];
        const structureStorage = await saveCostStructureDocument(database, orderId, itemCode, structure);
        delete refreshed.estrutura;
        refreshed.estruturaDocId = structureStorage.docId;
        refreshed.estruturaChunkCount = structureStorage.chunkCount;
        refreshed.estruturaCharacterCount = structureStorage.characterCount;
        entry.maxiprod = refreshed;
        entry.maxiprod.atualizadoAutomaticamenteEm = new Date().toISOString();
        summary.updated += 1;
        console.log("Atualização automática concluída", { orderId, itemCode });
      } catch (error) {
        summary.failed += 1;
        summary.errors.push({ orderId, itemCode, message: String(error?.message || error).slice(0, 300) });
        console.error("Atualização automática falhou", { orderId, itemCode, message: error?.message || String(error) });
      }
    }

    summary.finishedAt = new Date().toISOString();
    summary.skipped = Object.keys(costsData).length - entries.length;
    await dbRef.set({ costsData, maxiprodAutomaticSync: summary }, { merge: true });
    console.log("Atualização automática finalizada", summary);
  },
);

export const calcularCustoMaxiprod = onCall(
  {
    region: "southamerica-east1",
    timeoutSeconds: 300,
    memory: "512MiB",
    // A função callable recebe a sessão Firebase no corpo da chamada.
    // O Cloud Run precisa aceitar a requisição para que essa validação ocorra.
    invoker: "public",
    secrets: [MAXIPROD_TOKEN],
  },
  async (request) => {
    console.log("calcularCustoMaxiprod: início", { itemCode: request.data?.itemCode || null, authenticated: Boolean(request.auth) });
    if (!request.auth) {
      throw new HttpsError("unauthenticated", "Faça login no aplicativo para consultar custos.");
    }

    const itemCode = safeItemCode(request.data?.itemCode);
    try {
      const result = await calculateStructureCost(itemCode, MAXIPROD_TOKEN.value());
      console.log("calcularCustoMaxiprod: concluído", { itemCode, total: result.custoTotal, items: result.itensComCusto });
      return result;
    } catch (error) {
      console.error("calcularCustoMaxiprod: falhou", { itemCode, message: error?.message || String(error), stack: error?.stack || null });
      throw error;
    }
  },
);

export const consultarOrdemProducaoMaxiprod = onCall(
  {
    region: "southamerica-east1",
    timeoutSeconds: 120,
    memory: "512MiB",
    invoker: "public",
    secrets: [MAXIPROD_TOKEN],
  },
  async (request) => {
    console.log("consultarOrdemProducaoMaxiprod: início", { itemCode: request.data?.itemCode || null, authenticated: Boolean(request.auth) });
    if (!request.auth) {
      throw new HttpsError("unauthenticated", "Faça login no aplicativo para consultar a Ordem de Produção.");
    }
    const itemCode = safeItemCode(request.data?.itemCode);
    try {
      const result = await loadProductionOrder(itemCode, MAXIPROD_TOKEN.value());
      console.log("consultarOrdemProducaoMaxiprod: concluído", { itemCode, ordem: result.ordemProducao?.numero || null, insumos: result.insumos.length, operacoes: result.operacoes.length });
      return result;
    } catch (error) {
      console.error("consultarOrdemProducaoMaxiprod: falhou", { itemCode, message: error?.message || String(error), stack: error?.stack || null });
      throw error;
    }
  },
);

// Consulta financeira/fiscal genérica. O MaxiProd expõe pequenas diferenças de
// campos entre versões; por isso primeiro lemos o schema e, depois, consultamos
// somente os campos escalares que realmente estão disponíveis em cada coleção.
// As consultas são propositalmente sequenciais: nesta conta o GraphQL não aceita
// duas operações de banco concorrentes na mesma requisição.
const FINANCE_ROOTS = [
  "contaAReceber", "contaAPagar", "pedidosDeVenda", "pedidosDeCompra",
  "notasFiscais", "itensDasNotasFiscais", "itens", "estoques",
  "estoqueMovimentacoes", "operacoesFiscais", "contasContabeis",
  "lancamentosContabeis", "centrosDeCustos", "contatos",
];

function unwrapGraphqlType(type) {
  let current = type;
  while (current && (current.kind === "NON_NULL" || current.kind === "LIST")) current = current.ofType;
  return current?.name || null;
}

function scalarFieldNames(type) {
  const allowed = new Set(["SCALAR", "ENUM"]);
  return (type?.fields || [])
    .filter((field) => {
      let fieldType = field.type;
      while (fieldType && (fieldType.kind === "NON_NULL" || fieldType.kind === "LIST")) fieldType = fieldType.ofType;
      return allowed.has(fieldType?.kind);
    })
    .map((field) => field.name)
    .filter((name) => name && !name.startsWith("__"));
}

// Conta a receber possui campos relacionados (cliente, empresa, nota etc.) em
// algumas versoes do MaxiProd. Antes buscavamos apenas os escalares da propria
// conta, por isso a tela tinha o codigo interno, mas nao o nome do cliente.
// A selecao abaixo e montada pelo schema: so entra um relacionamento que de
// fato existe e somente com campos escalares, evitando consultas invalidas.
function financeRelatedSelection(itemType, typeMap) {
  // Contas a receber distribui os dados do título em relacionamentos
  // diferentes conforme a origem (pedido, proposta, NF, cobrança etc.).
  // Incluímos todos os relacionamentos financeiros possíveis para que o
  // front consiga exibir cliente, forma de cobrança e identificação.
  const allowedNames = /cliente|pessoa|empresa|contato|fornecedor|pedido|nota|cobranca|pagamento|conta|titulo|proposta|venda|sacado|pagador|parcela|referencia|historico/i;
  return (itemType?.fields || [])
    .filter((field) =>
      allowedNames.test(field.name || "") &&
      !/forma.*cobranca|cobranca.*forma/i.test(field.name || "")
    )
    .map((field) => {
      const relatedType = typeMap.get(unwrapGraphqlType(field.type));
      const fields = scalarFieldNames(relatedType)
        .filter((name) => /^(id|codigo|nome|apelido|descricao|razaoSocial|nomeRazaoSocial|nomeFantasia|nomeCompleto|sigla|identificacao|referencia|referenteA|historico|numero|parcela|numeroParcela|numeroTotalParcelas|quantidadeParcelas)$/i.test(name));
      return fields.length ? `${field.name} { ${fields.join(" ")} }` : "";
    })
    .filter(Boolean);
}

let financeSchemaCache = null;
let financeSchemaCacheAt = 0;
async function getFinanceSchema(token) {
  if (financeSchemaCache && Date.now() - financeSchemaCacheAt < 30 * 60 * 1000) {
    return financeSchemaCache;
  }
  const query = `
    query SchemaFinanceiro {
      __schema {
        queryType { fields { name type { kind name ofType { kind name ofType { kind name } } } } }
        types { name kind fields { name type { kind name ofType { kind name ofType { kind name } } } } }
      }
    }`;
  financeSchemaCache = await maxiprodQuery(query, {}, token);
  financeSchemaCacheAt = Date.now();
  return financeSchemaCache;
}

function normalizeFinanceKey(value) {
  return String(value || "").normalize("NFD").replace(/[\u0300-\u036f]/g, "").toLowerCase().replace(/[^a-z0-9]/g, "");
}

function findFinanceValue(source, aliases, depth = 0) {
  if (!source || typeof source !== "object" || depth > 3) return null;
  const wanted = aliases.map(normalizeFinanceKey);
  for (const [key, value] of Object.entries(source)) {
    if (value !== null && value !== undefined && value !== "" && wanted.includes(normalizeFinanceKey(key))) return value;
  }
  for (const value of Object.values(source)) {
    if (value && typeof value === "object") {
      const found = findFinanceValue(value, aliases, depth + 1);
      if (found !== null) return found;
    }
  }
  return null;
}

function financeText(value) {
  if (value === null || value === undefined || value === "") return null;
  if (typeof value !== "object") return String(value);
  const code = value.codigo || value.code || value.numero || "";
  const name = value.descricao || value.nome || value.nomeRazaoSocial || value.razaoSocial || value.identificacao || "";
  return [code, name].filter(Boolean).join(" - ") || null;
}

function financePartyText(value) {
  if (value === null || value === undefined || value === "") return null;
  if (typeof value !== "object") return String(value);
  const alias = value.apelido || value.codigo || value.sigla || value.code || "";
  const name = value.nomeRazaoSocial || value.razaoSocial || value.nomeFantasia || value.nomeCompleto || value.nome || "";
  return [...new Set([alias, name].filter(Boolean).map(String))].join(" - ") || financeText(value);
}

function stockUnitText(value) {
  if (value === null || value === undefined || value === "") return null;
  if (typeof value !== "object") return String(value);
  return String(value.sigla || value.abreviacao || value.simbolo || value.codigo || value.code || value.nome || "") || null;
}

function enrichReceivable(item) {
  const parcela = findFinanceValue(item, ["parcela", "numeroParcela", "numeroDaParcela", "parcelaNumero"]);
  const totalParcelas = findFinanceValue(item, ["numeroTotalParcelas", "numeroDeParcelas", "quantidadeParcelas", "totalParcelas", "numeroParcelas"]);
  const descricao = findFinanceValue(item, ["descricao", "descricaoTitulo", "descricaoDaConta", "historico", "referenteA", "referencia", "referente"]);
  return {
    ...item,
    descricaoDetalhe: financeText(descricao),
    parcelaDetalhe: financeText(parcela),
    totalParcelasDetalhe: financeText(totalParcelas),
  };
}

function enrichPayable(item) {
  const fornecedor = findFinanceValue(item, ["fornecedor", "credor", "favorecido"]);
  const descricao = findFinanceValue(item, ["descricao", "descricaoTitulo", "descricaoDaConta", "historico"]);
  const referenteA = findFinanceValue(item, ["referenteA", "referencia", "referente"]);
  return {
    ...item,
    valorGradeMaxiprod: payableGridAmount(item),
    fornecedorDetalhe: financePartyText(fornecedor),
    descricaoDetalhe: financeText(descricao),
    referenteADetalhe: financeText(referenteA),
  };
}

function financeTruthy(value) {
  if (value === true || value === 1) return true;
  return /^(1|true|sim|s|yes)$/i.test(String(value ?? "").trim());
}

function financeAmountOrNull(value) {
  if (value === null || value === undefined || value === "") return null;
  if (typeof value === "number") return Number.isFinite(value) ? Math.abs(value) : null;
  const text = String(value).replace(/[^0-9,.-]/g, "");
  if (!text) return null;
  const normalized = text.includes(",") ? text.replace(/\./g, "").replace(",", ".") : text;
  const number = Number(normalized);
  return Number.isFinite(number) ? Math.abs(number) : null;
}

function findFinanceValueByPriority(item, aliases) {
  for (const alias of aliases) {
    const value = findFinanceValue(item, [alias]);
    if (value !== null && value !== undefined && value !== "") return value;
  }
  return null;
}

function payableGridAmount(item) {
  const state = String(item?.estado || "").trim().toUpperCase();
  const paidState = ["PAGO", "LIQUIDADO", "QUITADO"].includes(state);
  const rawValue = financeAmountOrNull(findFinanceValueByPriority(item, ["valor"]));
  const liquidValue = financeAmountOrNull(findFinanceValueByPriority(
    item,
    ["valorLiquido", "valorOriginal"],
  ));
  const paidValue = financeAmountOrNull(findFinanceValueByPriority(
    item,
    ["valorPagoLiquido", "valorPago", "valorBaixado", "valorLiquidado"],
  ));
  const compensatedValue = financeAmountOrNull(findFinanceValueByPriority(
    item,
    ["valorCompensado", "adiantamentoValorCompensado"],
  ));
  if (paidState) {
    return Math.max((paidValue ?? liquidValue ?? rawValue ?? 0) - (compensatedValue || 0), 0);
  }

  const reportedBalance = financeAmountOrNull(findFinanceValueByPriority(
    item,
    ["valorAPagar", "saldoAPagar", "valorEmAberto", "saldo"],
  ));
  const candidates = [];
  if (reportedBalance !== null) candidates.push(reportedBalance);
  if (liquidValue !== null && (paidValue !== null || compensatedValue !== null)) {
    candidates.push(Math.max(liquidValue - (paidValue || 0) - (compensatedValue || 0), 0));
  }
  if (!candidates.length) candidates.push(rawValue ?? liquidValue ?? 0);
  return Math.min(...candidates);
}

function receivableIsAllowed(item) {
  // Espelha os filtros da grade: A receber + Recebidos e os tipos financeiros,
  // sem Adiantamentos compensados, Agrupados, Parcelados ou Propostas.
  const excludedStates = new Set(["COMPENSADO", "CANCELADO"]);
  const state = String(item?.estado || "").trim().toUpperCase();
  const type = String(item?.tipo || "").trim().toUpperCase();
  const isProposal = type.includes("PROPOSTA");
  const isUncheckedAuxiliaryType =
    type.includes("COMPENS") || type.includes("AGRUP") || type.includes("PARCELAD");
  const isFinancialType =
    type.includes("TITULO") ||
    type.includes("RECEITA") ||
    type.includes("ADIANTAMENTO") ||
    type.includes("PEDIDO_DE_VENDA");

  // Quando um pedido e faturado/baixado, a API conserva a previsao antiga como
  // TITULO_PEDIDO_DE_VENDA RECEBIDO. A grade do ERP a oculta e mostra apenas o
  // titulo/parcelas definitivos. Manter os dois era a origem das duplicidades.
  const isSettledSalesOrderForecast =
    state === "RECEBIDO" && type.includes("TITULO_PEDIDO_DE_VENDA");

  return !excludedStates.has(state) &&
    isFinancialType &&
    !isProposal &&
    !isUncheckedAuxiliaryType &&
    !isSettledSalesOrderForecast;
}

function payableIsAllowed(item) {
  // Espelha a grade: A pagar + Pagos; Títulos + Despesas +
  // Adiantamentos + Pedidos de compra. Compensados, agrupados,
  // parcelados e previsões antigas de pedidos já pagos não entram.
  const excludedStates = new Set(["COMPENSADO", "CANCELADO"]);
  const state = String(item?.estado || "").trim().toUpperCase();
  const type = String(item?.tipo || "").trim().toUpperCase();
  const isUncheckedAuxiliaryState =
    state.includes("COMPENS") || state.includes("AGRUP") || state.includes("PARCELAD");
  const isUncheckedAuxiliaryType =
    type.includes("COMPENS") || type.includes("AGRUP") || type.includes("PARCELAD");
  const isGrouped = financeTruthy(findFinanceValue(item, ["agrupado", "tituloAgrupado", "estaAgrupado"]));
  const isInstallmentAuxiliary = financeTruthy(findFinanceValue(
    item,
    ["parcelado", "tituloParcelado", "estaParcelado"],
  ));
  const isFinancialType =
    type.includes("TITULO") ||
    type.includes("DESPESA") ||
    type.includes("ADIANTAMENTO") ||
    type.includes("PEDIDO_DE_COMPRA");
  const isSettledPurchaseOrderForecast =
    ["PAGO", "LIQUIDADO", "QUITADO"].includes(state) &&
    type.includes("TITULO_PEDIDO_DE_COMPRA");

  return !excludedStates.has(state) &&
    !isUncheckedAuxiliaryState &&
    isFinancialType &&
    !isUncheckedAuxiliaryType &&
    !isSettledPurchaseOrderForecast &&
    !isGrouped &&
    !isInstallmentAuxiliary &&
    payableGridAmount(item) > 0.004;
}

function accountingReceivableIsAllowed(item) {
  // A Contabilidade mostra a condição inteira do pedido. Por isso, preserva
  // também as previsões do pedido que já foram recebidas.
  const state = String(item?.estado || "").trim().toUpperCase();
  const type = String(item?.tipo || "").trim().toUpperCase();
  const isFinancialType =
    type.includes("TITULO") ||
    type.includes("RECEITA") ||
    type.includes("ADIANTAMENTO") ||
    type.includes("PEDIDO_DE_VENDA");
  const isAuxiliary =
    type.includes("PROPOSTA") ||
    type.includes("COMPENS") ||
    type.includes("AGRUP") ||
    type.includes("PARCELAD");
  return !["COMPENSADO", "CANCELADO"].includes(state) && isFinancialType && !isAuxiliary;
}

function accountingPayableIsAllowed(item) {
  // Idem para compras: parcelas pagas continuam no histórico para que o
  // cabeçalho e a tabela representem toda a condição negociada.
  const state = String(item?.estado || "").trim().toUpperCase();
  const type = String(item?.tipo || "").trim().toUpperCase();
  const isFinancialType =
    type.includes("TITULO") ||
    type.includes("DESPESA") ||
    type.includes("ADIANTAMENTO") ||
    type.includes("PEDIDO_DE_COMPRA");
  const isAuxiliary =
    state.includes("COMPENS") || state.includes("AGRUP") || state.includes("PARCELAD") ||
    type.includes("COMPENS") || type.includes("AGRUP") || type.includes("PARCELAD") ||
    financeTruthy(findFinanceValue(item, ["agrupado", "tituloAgrupado", "estaAgrupado"])) ||
    financeTruthy(findFinanceValue(item, ["parcelado", "tituloParcelado", "estaParcelado"]));
  return !["COMPENSADO", "CANCELADO"].includes(state) && isFinancialType && !isAuxiliary;
}

function financeDateBoundary(value) {
  const text = String(value || "").trim();
  return /^\d{4}-\d{2}-\d{2}$/.test(text) ? `${text}T00:00:00.000-03:00` : null;
}

async function loadFinanceDataset(rootName, schema, token, take, skip = 0, dateFrom = null, dateTo = null, accountingMode = false) {
  const queryFields = schema?.__schema?.queryType?.fields || [];
  const typeMap = new Map((schema?.__schema?.types || []).map((type) => [type.name, type]));
  const root = queryFields.find((field) => field.name === rootName);
  const rootType = typeMap.get(unwrapGraphqlType(root?.type));
  const itemsField = rootType?.fields?.find((field) => field.name === "items");
  const itemType = typeMap.get(unwrapGraphqlType(itemsField?.type));
  const scalarNames = scalarFieldNames(itemType).filter((name) =>
    rootName !== "contaAReceber" || !/^forma(de)?cobranca|^formaDeCobranca/i.test(name)
  );
  if (!root || !itemsField || !scalarNames.length) {
    return { items: [], fields: [], available: false };
  }
  // Contas a receber e Contas a pagar dependem dos relacionamentos do título
  // para exibir cliente/fornecedor e os textos vistos na janela do MaxiProd.
  const relationSelection = rootName === "contaAReceber" || rootName === "contaAPagar"
    ? financeRelatedSelection(itemType, typeMap)
    : [];
  const selection = [...scalarNames, ...relationSelection].join("\n");
  // "skip" permite que o portal percorra todo o hist\u00f3rico em p\u00e1ginas, sem
  // tentar transferir milhares de lan\u00e7amentos em uma requisi\u00e7\u00e3o s\u00f3.
  // O filtro de estado e executado pelo MaxiProd antes da paginacao. O tipo nao
  // e limitado aqui porque a API possui subtipos de adiantamento/pedido que a
  // grade exibe, mas que eram perdidos pela lista fixa anterior.
  const fromBoundary = financeDateBoundary(dateFrom);
  const toBoundary = financeDateBoundary(dateTo);
  const whereParts = rootName === "contaAReceber" || rootName === "contaAPagar"
    ? ["estado: { nin: [COMPENSADO, CANCELADO] }"]
    : [];
  if ((rootName === "contaAReceber" || rootName === "contaAPagar") && fromBoundary) {
    const operators = [`gte: "${fromBoundary}"`];
    if (toBoundary) operators.push(`lt: "${toBoundary}"`);
    whereParts.push(`vencimentoData: { ${operators.join(", ")} }`);
  }
  const where = whereParts.length ? `, where: { ${whereParts.join(", ")} }` : "";
  const query = `query DadosFinanceiros { ${rootName}(take: ${take}, skip: ${skip}${where}) { items { ${selection} } } }`;
  const data = await maxiprodQuery(query, {}, token);
  const items = Array.isArray(data?.[rootName]?.items) ? data[rootName].items : [];
  const outputItems = rootName === "contaAReceber"
    ? items.filter(accountingMode ? accountingReceivableIsAllowed : receivableIsAllowed).map(enrichReceivable)
    : rootName === "contaAPagar"
      ? items.filter(accountingMode ? accountingPayableIsAllowed : payableIsAllowed).map(enrichPayable)
      : items;
  return {
    items: outputItems,
    rawCount: items.length,
    fields: scalarNames,
    available: true,
  };
}

export const consultarFinanceiroFiscalMaxiprod = onCall(
  {
    region: "southamerica-east1",
    timeoutSeconds: 300,
    memory: "512MiB",
    invoker: "public",
    secrets: [MAXIPROD_TOKEN],
  },
  async (request) => {
    if (!request.auth) {
      throw new HttpsError("unauthenticated", "Faça login no aplicativo para consultar os dados financeiros.");
    }
    // Uma chamada consulta somente uma fonte e uma p\u00e1gina. O navegador segue
    // as p\u00e1ginas para obter o hist\u00f3rico completo, mantendo cada resposta leve.
    const take = Math.min(Math.max(Number(request.data?.take) || 200, 20), 500);
    const skip = Math.max(Number(request.data?.skip) || 0, 0);
    const requestedSource = String(request.data?.source || '').trim();
    const dateFrom = String(request.data?.dateFrom || '').trim();
    const dateTo = String(request.data?.dateTo || '').trim();
    try {
      const schema = await getFinanceSchema(MAXIPROD_TOKEN.value());
      const datasets = {};
      const roots = requestedSource && FINANCE_ROOTS.includes(requestedSource)
        ? [requestedSource]
        : FINANCE_ROOTS;
      for (const rootName of roots) {
        try {
          datasets[rootName] = await loadFinanceDataset(
            rootName,
            schema,
            MAXIPROD_TOKEN.value(),
            take,
            skip,
            dateFrom,
            dateTo,
          );
        } catch (error) {
          // Uma coleção sem campo/filtro compatível não interrompe as demais.
          console.warn("Financeiro: coleção não consultada", { rootName, message: error?.message || String(error) });
          datasets[rootName] = { items: [], fields: [], available: false, error: String(error?.message || error).slice(0, 260) };
        }
      }
      const returned = requestedSource ? (datasets[requestedSource]?.rawCount ?? datasets[requestedSource]?.items?.length ?? 0) : 0;
      return {
        updatedAt: new Date().toISOString(),
        datasets,
        source: requestedSource || null,
        skip,
        take,
        done: Boolean(requestedSource) && returned < take,
      };
    } catch (error) {
      console.error("consultarFinanceiroFiscalMaxiprod: falhou", { message: error?.message || String(error), stack: error?.stack || null });
      throw error;
    }
  },
);

// Consulta exclusiva da aba Vendas. Ela usa o mesmo schema dinâmico das
// consultas financeiras, mas mantém a paginação e a normalização isoladas para
// que nenhuma alteração nesta tela interfira em Contas a receber/pagar.
function salesLocationSelection(type, typeMap, depth = 0, visited = new Set()) {
  if (!type || depth > 3 || visited.has(type.name)) return [];
  const nextVisited = new Set(visited);
  nextVisited.add(type.name);
  const scalars = scalarFieldNames(type).filter((name) =>
    /id|codigo|nome|apelido|razao|fantasia|completo|sigla|regiao|municipio|cidade|localidade|uf|estado|logradouro|bairro|cep/i.test(name)
  );
  const relations = (type.fields || [])
    .filter((field) => /endereco|local|municipio|cidade|estado|regiao|uf|^(items|nodes)$/i.test(field.name || ""))
    .map((field) => {
      const nestedType = typeMap.get(unwrapGraphqlType(field.type));
      const nestedSelection = salesLocationSelection(nestedType, typeMap, depth + 1, nextVisited);
      return nestedSelection.length ? `${field.name} { ${nestedSelection.join(" ")} }` : "";
    })
    .filter(Boolean);
  return [...new Set([...scalars, ...relations])];
}

function salesRelatedSelection(itemType, typeMap) {
  return (itemType?.fields || [])
    .filter((field) => /cliente|pessoa|contato|comprador|pagador|endereco|local|municipio|cidade|regiao/i.test(field.name || ""))
    .map((field) => {
      const relatedType = typeMap.get(unwrapGraphqlType(field.type));
      const selection = salesLocationSelection(relatedType, typeMap);
      return selection.length ? `${field.name} { ${selection.join(" ")} }` : "";
    })
    .filter(Boolean);
}

function salesStateRelatedSelection(itemType, typeMap) {
  return (itemType?.fields || [])
    .filter((field) => /estado|status|situacao|cancel/i.test(field.name || ""))
    .map((field) => {
      const relatedType = typeMap.get(unwrapGraphqlType(field.type));
      const fields = scalarFieldNames(relatedType)
        .filter((name) => /^(id|codigo|nome|descricao|valor|sigla|estado|status|situacao|cancelado|cancelada)$/i.test(name));
      return fields.length ? `${field.name} { ${fields.join(" ")} }` : "";
    })
    .filter(Boolean);
}

// Datas de entrega podem estar no cabeçalho do pedido ou somente nas linhas da
// grade de produtos/serviços. A seleção é montada pelo schema para aceitar as
// duas formas sem depender de um nome fixo de relacionamento.
function orderItemDeliveryFields(type, typeMap, depth = 0, visited = new Set()) {
  if (!type || depth > 3 || visited.has(type.name)) return [];
  const nextVisited = new Set(visited);
  nextVisited.add(type.name);
  const scalars = scalarFieldNames(type).filter((name) =>
    /^(id|codigo|numero|descricao|nome|entrega|entregaData|dataEntrega|previsao|previsaoData|previsaoEntrega|previsaoEntregaData|previsaoDeEntregaData|dataPrevistaEntrega)$/i.test(name)
  );
  const relations = (type.fields || [])
    .filter((field) => /^(items|nodes|item|produto|servico|produtoServico|linha|linhas|detalhe|detalhes)$/i.test(field.name || ""))
    .map((field) => {
      const nestedType = typeMap.get(unwrapGraphqlType(field.type));
      const nested = orderItemDeliveryFields(nestedType, typeMap, depth + 1, nextVisited);
      return nested.length ? `${field.name} { ${nested.join(" ")} }` : "";
    })
    .filter(Boolean);
  return [...new Set([...scalars, ...relations])];
}

function orderItemDeliverySelection(itemType, typeMap) {
  return (itemType?.fields || [])
    .filter((field) => /itens|items|produtos|servicos|linhas|detalhes|componentes/i.test(field.name || ""))
    .map((field) => {
      const relatedType = typeMap.get(unwrapGraphqlType(field.type));
      const selection = orderItemDeliveryFields(relatedType, typeMap);
      return selection.length ? `${field.name} { ${selection.join(" ")} }` : "";
    })
    .filter(Boolean);
}

function orderDeliveryDates(item) {
  const values = [];
  const add = (value) => {
    if (value === null || value === undefined || value === "" || typeof value === "object") return;
    const text = String(value).trim();
    if (/^\d{4}-\d{2}-\d{2}/.test(text) || /^\d{2}\/\d{2}\/\d{4}/.test(text)) values.push(text);
  };
  const visit = (source, depth = 0) => {
    if (!source || typeof source !== "object" || depth > 7) return;
    if (Array.isArray(source)) {
      source.forEach((entry) => visit(entry, depth + 1));
      return;
    }
    for (const [key, value] of Object.entries(source)) {
      const normalized = normalizeFinanceKey(key);
      if (/entrega/.test(normalized) && /(data|previsao|entrega)/.test(normalized)) add(value);
      if (value && typeof value === "object") visit(value, depth + 1);
    }
  };
  visit(item);
  return [...new Set(values)];
}

function salesOrderStateText(item) {
  const values = [];
  const visit = (source, depth = 0) => {
    if (!source || typeof source !== "object" || depth > 3) return;
    for (const [key, value] of Object.entries(source)) {
      const normalizedKey = normalizeFinanceKey(key);
      if (/estado|status|situacao|cancelad|cancelled/.test(normalizedKey)) {
        if (value && typeof value === "object") {
          values.push(financeText(value) || JSON.stringify(value));
        } else if (value !== null && value !== undefined && value !== "") {
          values.push(String(value));
        }
      }
      if (value && typeof value === "object") visit(value, depth + 1);
    }
  };
  visit(item);
  return values.filter(Boolean).join(" | ");
}

function orderMainStateText(item) {
  // Usa somente campos do cabeçalho. Estados das linhas/itens não podem
  // transformar um pedido ainda aprovado em recebido/faturado por engano.
  const values = [];
  const accepted = new Set([
    "estado", "status", "situacao", "estadopedido", "statuspedido",
    "estadodescricao", "statusdescricao", "cancelado", "cancelada",
  ]);
  for (const [key, value] of Object.entries(item || {})) {
    if (!accepted.has(normalizeFinanceKey(key)) || value === null || value === undefined || value === "") continue;
    values.push(typeof value === "object" ? (financeText(value) || JSON.stringify(value)) : String(value));
  }
  return values.filter(Boolean).join(" | ");
}

function findSalesLocationValue(source, keyPattern, depth = 0) {
  if (!source || typeof source !== "object" || depth > 6) return null;
  for (const [key, value] of Object.entries(source)) {
    if (value !== null && value !== undefined && value !== "" && keyPattern.test(normalizeFinanceKey(key))) return value;
  }
  for (const value of Object.values(source)) {
    if (value && typeof value === "object") {
      const found = findSalesLocationValue(value, keyPattern, depth + 1);
      if (found !== null) return found;
    }
  }
  return null;
}

function salesUfText(value) {
  if (value === null || value === undefined || value === "") return null;
  if (typeof value === "object") {
    return salesUfText(
      value.sigla ?? value.uf ?? value.estadoUf ?? value.codigo ?? value.code ?? value.nome ?? value.descricao,
    );
  }
  const text = String(value).normalize("NFD").replace(/[\u0300-\u036f]/g, "").trim().toUpperCase();
  const aliases = {
    ACRE: "AC", ALAGOAS: "AL", AMAPA: "AP", AMAZONAS: "AM", BAHIA: "BA", CEARA: "CE",
    "DISTRITO FEDERAL": "DF", "ESPIRITO SANTO": "ES", GOIAS: "GO", MARANHAO: "MA",
    "MATO GROSSO": "MT", "MATO GROSSO DO SUL": "MS", "MINAS GERAIS": "MG", PARA: "PA",
    PARAIBA: "PB", PARANA: "PR", PERNAMBUCO: "PE", PIAUI: "PI", "RIO DE JANEIRO": "RJ",
    "RIO GRANDE DO NORTE": "RN", "RIO GRANDE DO SUL": "RS", RONDONIA: "RO", RORAIMA: "RR",
    "SANTA CATARINA": "SC", "SAO PAULO": "SP", SERGIPE: "SE", TOCANTINS: "TO",
  };
  if (aliases[text]) return aliases[text];
  const match = text.match(/(?:^|[^A-Z])(AC|AL|AP|AM|BA|CE|DF|ES|GO|MA|MT|MS|MG|PA|PB|PR|PE|PI|RJ|RN|RS|RO|RR|SC|SP|SE|TO)(?:$|[^A-Z])/);
  return match?.[1] || null;
}

function enrichSalesOrder(item, contactLocations = new Map()) {
  const cliente = findFinanceValue(item, ["cliente", "pessoa", "comprador", "pagador"]);
  const clienteId = findFinanceValue(item, ["clienteId", "pessoaId", "contatoId"])
    ?? findFinanceValue(cliente, ["id", "clienteId", "pessoaId", "contatoId"]);
  const contactLocation = clienteId !== null && clienteId !== undefined
    ? contactLocations.get(String(clienteId))
    : null;
  const locationSource = contactLocation || cliente || item;
  const regiao = findFinanceValue(item, ["regiao", "regiaoNome", "macroRegiao"])
    ?? findFinanceValue(locationSource, ["regiao", "regiaoNome", "macroRegiao"])
    ?? findSalesLocationValue(locationSource, /regiao/);
  // A grade de Pedidos de venda do MaxiProd expõe UF diretamente no pedido.
  // Esse campo deve ter prioridade sobre o cadastro do cliente, que pode não
  // estar acessível para a credencial da integração (permissão EMP000).
  const municipio = findFinanceValue(item, ["municipioNome", "cidadeNome", "municipio", "cidade", "localidade"])
    ?? findFinanceValue(locationSource, ["municipioNome", "cidadeNome", "municipio", "cidade", "localidade"])
    ?? findSalesLocationValue(locationSource, /municipio|cidade|localidade/);
  const uf = findFinanceValue(item, ["uf", "ufPedido", "estadoUf", "estadoSigla", "siglaEstado"])
    ?? findSalesLocationValue(item, /^(uf|ufpedido|estadouf|estadosigla|siglaestado)$/)
    ?? findFinanceValue(locationSource, ["uf", "estadoSigla", "siglaEstado"])
    ?? findSalesLocationValue(locationSource, /^(uf|estadosigla|siglaestado)$/)
    ?? findFinanceValue(findFinanceValue(locationSource, ["estado"]), ["uf", "sigla", "codigo"]);
  const deliveryDates = orderDeliveryDates(item);
  const directDelivery = findFinanceValue(item, [
    "entregaData", "dataEntrega", "previsaoDeEntregaData",
    "previsaoEntregaData", "dataPrevistaEntrega", "entrega",
  ]);
  return {
    id: findFinanceValue(item, ["id", "pedidoDeVendaId"]),
    emissaoDataDetalhe: findFinanceValue(item, ["emissaoData", "dataEmissao", "emissao"]),
    numeroDetalhe: findFinanceValue(item, ["numero", "numeroPedido", "pedidoNumero"]),
    referenciaDetalhe: financeText(findFinanceValue(item, ["referencia", "descricao", "observacao"])),
    clienteDetalhe: financePartyText(cliente) || financePartyText(contactLocation),
    valorTotalDetalhe: findFinanceValue(item, ["valorTotal", "total", "valor"]),
    valorFaturadoDetalhe: findFinanceValue(item, ["valorFaturado", "totalFaturado", "faturadoValor"]),
    valorAFaturarDetalhe: findFinanceValue(item, [
      "valorAFaturar", "totalAFaturar", "saldoAFaturar",
      "valorNaoFaturado", "valorRestanteAFaturar",
    ]),
    entregaDataDetalhe: directDelivery ?? deliveryDates[0] ?? null,
    entregasItensDetalhe: deliveryDates,
    regiaoDetalhe: financeText(regiao),
    municipioDetalhe: financeText(municipio),
    ufDetalhe: salesUfText(uf),
    localizacaoDiagnostico: {
      clienteId,
      origem: contactLocation ? "contatos" : cliente ? "pedidoDeVenda.cliente" : "pedidoDeVenda",
      regiao: financeText(regiao),
      municipio: financeText(municipio),
      uf: salesUfText(uf),
    },
    estadoPrincipalDetalhe: orderMainStateText(item),
    estadoDetalhe: salesOrderStateText(item),
  };
}

let salesContactLocationsCache = null;
let salesContactLocationsCacheAt = 0;
async function loadSalesContactLocations(schema, token) {
  if (salesContactLocationsCache && Date.now() - salesContactLocationsCacheAt < 30 * 60 * 1000) {
    return salesContactLocationsCache;
  }
  const queryFields = schema?.__schema?.queryType?.fields || [];
  const typeMap = new Map((schema?.__schema?.types || []).map((type) => [type.name, type]));
  const root = queryFields.find((field) => /^(contatos|clientes|pessoas)$/i.test(field.name || ""));
  const rootType = typeMap.get(unwrapGraphqlType(root?.type));
  const itemsField = rootType?.fields?.find((field) => field.name === "items");
  const itemType = typeMap.get(unwrapGraphqlType(itemsField?.type));
  if (!root || !itemsField || !itemType) return new Map();
  const selection = [...new Set([
    ...scalarFieldNames(itemType),
    ...salesLocationSelection(itemType, typeMap),
  ])].join("\n");
  const locations = new Map();
  const take = 500;
  try {
    for (let page = 0; page < 20; page += 1) {
      const skip = page * take;
      const query = `query ContatosLocalizacaoVendas {
        ${root.name}(take: ${take}, skip: ${skip}) {
          items { ${selection} }
        }
      }`;
      const data = await maxiprodQuery(query, {}, token);
      const items = Array.isArray(data?.[root.name]?.items) ? data[root.name].items : [];
      items.forEach((contact) => {
        const id = findFinanceValue(contact, ["id", "clienteId", "pessoaId", "contatoId"]);
        if (id !== null && id !== undefined) locations.set(String(id), contact);
      });
      if (items.length < take) break;
    }
    salesContactLocationsCache = locations;
    salesContactLocationsCacheAt = Date.now();
    console.log("Vendas: localizacoes de contatos carregadas", { root: root.name, contatos: locations.size });
    return locations;
  } catch (error) {
    console.warn("Vendas: token sem acesso à localização dos contatos; usando somente os campos do pedido", {
      root: root.name,
      message: error?.message || String(error),
    });
    salesContactLocationsCache = new Map();
    salesContactLocationsCacheAt = Date.now();
    return salesContactLocationsCache;
  }
}

function salesOrderIsAllowed(item) {
  if (item?.cancelado === true || item?.cancelada === true || item?.isCancelled === true) return false;
  const state = (orderMainStateText(item) || salesOrderStateText(item))
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .trim()
    .toUpperCase();
  return !state.includes("CANCELAD") && !state.includes("CANCELLED");
}

async function loadSalesOrdersDataset(schema, token, take, skip) {
  const queryFields = schema?.__schema?.queryType?.fields || [];
  const typeMap = new Map((schema?.__schema?.types || []).map((type) => [type.name, type]));
  const root = queryFields.find((field) => field.name === "pedidosDeVenda");
  const rootType = typeMap.get(unwrapGraphqlType(root?.type));
  const itemsField = rootType?.fields?.find((field) => field.name === "items");
  const itemType = typeMap.get(unwrapGraphqlType(itemsField?.type));
  const scalarNames = scalarFieldNames(itemType);
  if (!root || !itemsField || !scalarNames.length) {
    throw new HttpsError("failed-precondition", "A coleção Pedidos de venda não está disponível no MaxiProd.");
  }
  const baseSelection = [...new Set([
    ...scalarNames,
    ...salesLocationSelection(itemType, typeMap),
    ...salesRelatedSelection(itemType, typeMap),
    ...salesStateRelatedSelection(itemType, typeMap),
  ])];
  const deliverySelection = orderItemDeliverySelection(itemType, typeMap);
  const run = async (includeItemDeliveries) => {
    const selection = [...baseSelection, ...(includeItemDeliveries ? deliverySelection : [])].join("\n");
    const query = `query PedidosVendaApp {
      pedidosDeVenda(take: ${take}, skip: ${skip}) {
        items { ${selection} }
      }
    }`;
    return maxiprodQuery(query, {}, token);
  };
  let data;
  try {
    data = await run(true);
  } catch (error) {
    if (!deliverySelection.length) throw error;
    console.warn("Pedidos de venda: entrega dos itens não disponível; usando somente o cabeçalho.", error?.message || error);
    data = await run(false);
  }
  const items = Array.isArray(data?.pedidosDeVenda?.items) ? data.pedidosDeVenda.items : [];
  const contactLocations = await loadSalesContactLocations(schema, token);
  return {
    items: items.filter(salesOrderIsAllowed).map((item) => enrichSalesOrder(item, contactLocations)),
    rawCount: items.length,
    fields: scalarNames,
    deliveryFields: deliverySelection,
  };
}

export const consultarPedidosVendaMaxiprod = onCall(
  {
    region: "southamerica-east1",
    timeoutSeconds: 300,
    memory: "512MiB",
    invoker: "public",
    secrets: [MAXIPROD_TOKEN],
  },
  async (request) => {
    if (!request.auth) {
      throw new HttpsError("unauthenticated", "Faça login no aplicativo para consultar os pedidos de venda.");
    }
    const take = Math.min(Math.max(Number(request.data?.take) || 200, 20), 500);
    const skip = Math.max(Number(request.data?.skip) || 0, 0);
    try {
      const schema = await getFinanceSchema(MAXIPROD_TOKEN.value());
      const dataset = await loadSalesOrdersDataset(schema, MAXIPROD_TOKEN.value(), take, skip);
      return {
        updatedAt: new Date().toISOString(),
        items: dataset.items,
        fields: dataset.fields,
        skip,
        take,
        done: dataset.rawCount < take,
      };
    } catch (error) {
      console.error("consultarPedidosVendaMaxiprod: falhou", {
        message: error?.message || String(error),
        stack: error?.stack || null,
      });
      if (error instanceof HttpsError) throw error;
      throw new HttpsError("internal", "Não foi possível consultar os pedidos de venda no MaxiProd.");
    }
  },
);

// Pedidos de compra em aberto. A consulta filtra no MaxiProd os estados finais
// para não transferir todo o histórico de pedidos já recebidos ou cancelados.
function purchaseOrderPartySelection(itemType, typeMap) {
  return (itemType?.fields || [])
    .filter((field) => /fornecedor|pessoa|contato|empresa/i.test(field.name || ""))
    .map((field) => {
      const relatedType = typeMap.get(unwrapGraphqlType(field.type));
      const fields = scalarFieldNames(relatedType)
        .filter((name) => /^(id|codigo|nome|apelido|razaoSocial|nomeRazaoSocial|nomeFantasia|nomeCompleto|sigla)$/i.test(name));
      return fields.length ? `${field.name} { ${fields.join(" ")} }` : "";
    })
    .filter(Boolean);
}

function purchaseOrderAttachmentSelection(itemType, typeMap) {
  return (itemType?.fields || [])
    .filter((field) => /anex|arquivo|documento/i.test(field.name || ""))
    .map((field) => {
      const relatedType = typeMap.get(unwrapGraphqlType(field.type));
      if (!relatedType) return "";
      const direct = scalarFieldNames(relatedType)
        .filter((name) => /^(id|nome|name|arquivoNome|fileName|quantidade|total|totalCount|count)$/i.test(name));
      const nested = (relatedType.fields || [])
        .filter((nestedField) => /^(items|nodes|anexos|arquivos|documentos)$/i.test(nestedField.name || ""))
        .map((nestedField) => {
          const nestedType = typeMap.get(unwrapGraphqlType(nestedField.type));
          const fields = scalarFieldNames(nestedType)
            .filter((name) => /^(id|nome|name|arquivoNome|fileName)$/i.test(name));
          return fields.length ? `${nestedField.name} { ${fields.join(" ")} }` : "";
        })
        .filter(Boolean);
      const fields = [...direct, ...nested];
      return fields.length ? `${field.name} { ${fields.join(" ")} }` : "";
    })
    .filter(Boolean);
}

function purchaseOrderAttachmentCount(item) {
  const explicit = findFinanceValueByPriority(item, [
    "anexosQuantidade", "quantidadeAnexos", "arquivosQuantidade",
    "quantidadeArquivos", "anexosTotal", "arquivosTotal",
  ]);
  if (explicit !== null && explicit !== undefined && explicit !== "" && Number.isFinite(Number(explicit))) {
    return Math.max(0, Number(explicit));
  }
  const ids = new Set();
  let anonymous = 0;
  const visit = (source, attachmentContext = false, depth = 0) => {
    if (!source || depth > 5) return;
    if (Array.isArray(source)) {
      if (attachmentContext) {
        for (const entry of source) {
          const id = entry && typeof entry === "object" ? (entry.id ?? entry.nome ?? entry.name ?? entry.arquivoNome ?? entry.fileName) : entry;
          if (id !== null && id !== undefined && id !== "") ids.add(String(id));
          else anonymous++;
        }
      }
      source.forEach((entry) => visit(entry, attachmentContext, depth + 1));
      return;
    }
    if (typeof source !== "object") return;
    for (const [key, value] of Object.entries(source)) {
      const keyIsAttachment = /anex|arquivo|documento/i.test(key);
      const childContext = attachmentContext || keyIsAttachment;
      if (childContext && /^(total|totalCount|count|quantidade)$/i.test(key) && Number.isFinite(Number(value))) {
        anonymous = Math.max(anonymous, Number(value));
      }
      visit(value, childContext, depth + 1);
    }
  };
  visit(item);
  return Math.max(ids.size, anonymous);
}

function purchaseOrderIsOpen(item) {
  const state = (orderMainStateText(item) || salesOrderStateText(item))
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^A-Za-z0-9]/g, "")
    .toUpperCase();
  if (state.includes("CANCEL") || state.includes("EXCLUID")) return false;
  if (state.includes("FATURADO") && !state.includes("ENTREGAFUTURA")) return false;
  if (state.includes("RECEBIDO") && !state.includes("ENTREGAFUTURA")) return false;
  return true;
}

function enrichPurchaseOrder(item) {
  const fornecedor = findFinanceValue(item, ["fornecedor", "pessoa", "contato", "empresa"]);
  const deliveryDates = orderDeliveryDates(item);
  const directDelivery = findFinanceValue(item, [
    "previsaoDeEntregaData", "previsaoEntregaData", "dataPrevistaEntrega",
    "entregaData", "dataEntrega", "entrega",
  ]);
  return {
    id: findFinanceValue(item, ["id", "pedidoDeCompraId"]),
    emissaoDataDetalhe: findFinanceValue(item, ["emissaoData", "dataEmissao", "emissao"]),
    numeroDetalhe: findFinanceValue(item, ["numero", "numeroPedido", "pedidoNumero"]),
    fornecedorDetalhe: financePartyText(fornecedor),
    observacaoDetalhe: financeText(findFinanceValue(item, ["observacoes", "observacao", "observacoesInternas", "comentario", "referencia"])),
    valorTotalDetalhe: findFinanceValue(item, ["valorTotal", "total", "valor"]),
    previsaoEntregaDataDetalhe: directDelivery ?? deliveryDates[0] ?? null,
    entregasItensDetalhe: deliveryDates,
    estadoPrincipalDetalhe: orderMainStateText(item),
    estadoDetalhe: salesOrderStateText(item),
    anexosQuantidadeDetalhe: purchaseOrderAttachmentCount(item),
  };
}

async function loadPurchaseOrdersDataset(schema, token, take, skip) {
  const queryFields = schema?.__schema?.queryType?.fields || [];
  const typeMap = new Map((schema?.__schema?.types || []).map((type) => [type.name, type]));
  const root = queryFields.find((field) => field.name === "pedidosDeCompra");
  const rootType = typeMap.get(unwrapGraphqlType(root?.type));
  const itemsField = rootType?.fields?.find((field) => field.name === "items");
  const itemType = typeMap.get(unwrapGraphqlType(itemsField?.type));
  const scalarNames = scalarFieldNames(itemType);
  if (!root || !itemsField || !scalarNames.length) {
    throw new HttpsError("failed-precondition", "A coleção Pedidos de compra não está disponível no MaxiProd.");
  }
  const baseSelection = [
    ...scalarNames,
    ...purchaseOrderPartySelection(itemType, typeMap),
    ...salesStateRelatedSelection(itemType, typeMap),
  ];
  const attachmentSelection = purchaseOrderAttachmentSelection(itemType, typeMap);
  const deliverySelection = orderItemDeliverySelection(itemType, typeMap);
  const run = async (includeAttachments, includeItemDeliveries) => {
    const selection = [
      ...baseSelection,
      ...(includeAttachments ? attachmentSelection : []),
      ...(includeItemDeliveries ? deliverySelection : []),
    ].join("\n");
    const query = `query PedidosCompraAbertosApp {
      pedidosDeCompra(
        take: ${take}
        skip: ${skip}
        where: { estado: { nin: [RECEBIDO, CANCELADO] } }
      ) { items { ${selection} } }
    }`;
    return maxiprodQuery(query, {}, token);
  };
  let data;
  try {
    data = await run(true, true);
  } catch (error) {
    console.warn("Pedidos de compra: repetindo sem anexos.", error?.message || error);
    try {
      data = await run(false, true);
    } catch (deliveryError) {
      if (!deliverySelection.length && !attachmentSelection.length) throw deliveryError;
      console.warn("Pedidos de compra: entrega dos itens não disponível; usando somente o cabeçalho.", deliveryError?.message || deliveryError);
      data = await run(false, false);
    }
  }
  const items = Array.isArray(data?.pedidosDeCompra?.items) ? data.pedidosDeCompra.items : [];
  return {
    items: items.filter(purchaseOrderIsOpen).map(enrichPurchaseOrder),
    rawCount: items.length,
    fields: scalarNames,
    attachmentFields: attachmentSelection,
    deliveryFields: deliverySelection,
  };
}

export const consultarPedidosCompraMaxiprod = onCall(
  {
    region: "southamerica-east1",
    timeoutSeconds: 300,
    memory: "512MiB",
    invoker: "public",
    secrets: [MAXIPROD_TOKEN],
  },
  async (request) => {
    if (!request.auth) {
      throw new HttpsError("unauthenticated", "Faça login no aplicativo para consultar os pedidos de compra.");
    }
    const take = Math.min(Math.max(Number(request.data?.take) || 200, 20), 500);
    const skip = Math.max(Number(request.data?.skip) || 0, 0);
    try {
      const schema = await getFinanceSchema(MAXIPROD_TOKEN.value());
      const dataset = await loadPurchaseOrdersDataset(schema, MAXIPROD_TOKEN.value(), take, skip);
      return {
        updatedAt: new Date().toISOString(),
        items: dataset.items,
        fields: dataset.fields,
        attachmentFields: dataset.attachmentFields,
        skip,
        take,
        done: dataset.rawCount < take,
      };
    } catch (error) {
      console.error("consultarPedidosCompraMaxiprod: falhou", {
        message: error?.message || String(error),
        stack: error?.stack || null,
      });
      if (error instanceof HttpsError) throw error;
      throw new HttpsError("internal", "Não foi possível consultar os pedidos de compra no MaxiProd.");
    }
  },
);

// Ativos imobilizados: posições de estoque cujos itens pertencem ao grupo 8.
// A seleção é montada pelo schema para aceitar variações de nomes entre
// versões do MaxiProd sem alterar as demais integrações.
function fixedAssetRelatedSelection(stockType, typeMap) {
  return (stockType?.fields || [])
    .filter((field) => /item|produto|material|grupo|conta|estoque/i.test(field.name || ""))
    .map((field) => {
      const relatedType = typeMap.get(unwrapGraphqlType(field.type));
      const fields = scalarFieldNames(relatedType).filter((name) =>
        /^(id|codigo|nome|descricao|apelido|nomeFantasia|razaoSocial|sigla|abreviacao|simbolo|grupo|grupoId|unidade|unid|unidadeDeMedida|siglaUnidade|criacaoData|criadoEm|dataCriacao)$/i.test(name)
      );
      const nested = (relatedType?.fields || [])
        .filter((nestedField) => /grupo|unidade|medida/i.test(nestedField.name || ""))
        .map((nestedField) => {
          const nestedType = typeMap.get(unwrapGraphqlType(nestedField.type));
          const nestedScalars = scalarFieldNames(nestedType)
            .filter((name) => /^(id|codigo|nome|descricao|sigla|abreviacao|simbolo)$/i.test(name));
          return nestedScalars.length ? `${nestedField.name} { ${nestedScalars.join(" ")} }` : "";
        })
        .filter(Boolean);
      const selection = [...fields, ...nested];
      return selection.length ? `${field.name} { ${selection.join(" ")} }` : "";
    })
    .filter(Boolean);
}

function findFinanceValueByKeyPattern(source, patterns, depth = 0) {
  if (!source || typeof source !== "object" || depth > 3) return null;
  for (const [key, value] of Object.entries(source)) {
    const normalized = normalizeFinanceKey(key);
    if (value !== null && value !== undefined && value !== "" && patterns.some((pattern) => pattern.test(normalized))) {
      return value;
    }
  }
  for (const value of Object.values(source)) {
    if (value && typeof value === "object") {
      const found = findFinanceValueByKeyPattern(value, patterns, depth + 1);
      if (found !== null) return found;
    }
  }
  return null;
}

function fixedAssetGroupCode(item) {
  const group = findFinanceValue(item, ["grupo", "grupoCodigo", "codigoGrupo", "grupoDeItem"]);
  if (group && typeof group === "object") return group.codigo ?? group.id ?? group.nome ?? group.descricao ?? null;
  return group;
}

function fixedAssetIsGroupEight(item) {
  const value = String(fixedAssetGroupCode(item) ?? "").trim();
  return value === "8" || /^8(?:\D|$)/.test(value);
}

function stockItemIsGroup(item, groupCode) {
  const value = String(fixedAssetGroupCode(item) ?? "").trim();
  const expected = String(groupCode ?? "").trim();
  return value === expected || new RegExp(`^${expected}(?:\\D|$)`).test(value);
}

function enrichFixedAsset(item) {
  const product = findFinanceValue(item, ["item", "produto", "material"]);
  const stockAccount = findFinanceValue(item, [
    "conta", "contaDeEstoque", "contaEstoque", "contaContabil",
    "contaContabilDeEstoque", "estoque", "contaId", "contaDeEstoqueId",
    "contaEstoqueId", "contaContabilId", "estoqueContaId",
  ]);
  const quantityRaw =
    findFinanceValue(item, [
      "quantidade", "quantidadeAtual", "quantidadeEmEstoque",
      "estoqueQuantidade", "saldoAtual", "saldoQuantidade",
      "quantidadeDisponivel", "saldo", "quantidadeTotal",
      "quantidadeTotalEmEstoque", "quantidadeEmUnidades", "saldoTotal",
    ]) ??
    findFinanceValueByKeyPattern(item, [
      /^quantidade$/, /^quantidade.*estoque$/,
      /^quantidade.*total$/, /^estoque.*quantidade$/, /^saldo.*quantidade$/,
      /^saldo.*atual$/, /^saldo.*total$/,
    ]);
  const unitValueRaw =
    findFinanceValue(item, [
      "custoUnitario", "valorUnitario", "custoMedioUnitario",
      "custoDeEstoqueUnitario", "custoEstoqueUnitario",
      "custoMedio", "custoMedioValor", "valorDoCustoUnitario",
      "custoValorUnitario", "precoUnitario",
    ]) ??
    findFinanceValueByKeyPattern(item, [
      /^custo.*unitario$/, /^valor.*unitario$/, /^preco.*unitario$/,
      /^custo.*medio$/, /^custo.*medio.*valor$/, /^valor.*custo.*unitario$/,
    ]);
  const stockTotalRaw =
    findFinanceValue(item, [
      "custoTotal", "valorTotal", "valorEmEstoque",
      "valorDoEstoque", "custoEstoqueTotal", "custoTotalValor",
    ]) ??
    findFinanceValueByKeyPattern(item, [
      /^custo.*total$/, /^valor.*estoque$/,
      /^estoque.*valor.*total$/, /^custo.*total.*valor$/,
    ]);
  const quantity = financeAmountOrNull(quantityRaw) ?? 0;
  const stockTotal = financeAmountOrNull(stockTotalRaw);
  const unitValue =
    financeAmountOrNull(unitValueRaw) ??
    (stockTotal !== null && quantity !== 0 ? stockTotal / quantity : 0);
  return {
    id: findFinanceValue(item, ["id", "estoqueId"]),
    criadoEm:
      findFinanceValue(product, ["criacaoData", "criadoEm", "dataCriacao"]) ??
      findFinanceValue(item, ["criacaoData", "criadoEm", "dataCriacao"]),
    codigo: findFinanceValue(product, ["codigo", "itemCodigo"]) ?? findFinanceValue(item, ["itemCodigo", "codigo"]),
    descricao: financeText(
      findFinanceValue(product, ["descricao", "nome", "itemDescricao"]) ??
      findFinanceValue(item, ["itemDescricao", "descricao", "nome"]),
    ),
    unidade: stockUnitText(
      findFinanceValue(product, ["unidade", "unid", "unidadeDeMedida", "siglaUnidade"]) ??
      findFinanceValue(item, ["unidade", "unid", "unidadeDeMedida", "siglaUnidade"]),
    ),
    estoque: financePartyText(stockAccount),
    quantidade: quantity,
    valorUnitario: unitValue,
    valorEstoque: stockTotal ?? quantity * unitValue,
  };
}

function findStockRoot(queryFields, typeMap) {
  return (queryFields || [])
    .map((field) => {
      const connectionType = typeMap.get(unwrapGraphqlType(field.type));
      const hasItems = Boolean(connectionType?.fields?.some((candidate) => candidate.name === "items"));
      const normalized = normalizeFinanceKey(field.name);
      let score = 0;
      // Usamos as posicoes analiticas de estoque e reproduzimos no app o
      // agrupamento da grade por Item + Conta. A colecao sintetica elimina
      // ajustes de estoque e nao expoe a quantidade corretamente.
      if (normalized === "estoque") score = 200;
      else if (normalized === "estoques") score = 195;
      else if (normalized.includes("estoque") && /agrup|consolidad|sintetic/.test(normalized)) score = 90;
      else if (/saldo.*estoque|estoque.*item|itens.*estoque/.test(normalized)) score = 80;
      else if (normalized.includes("estoque") && !/endereco|moviment|historico/.test(normalized)) score = 60;
      return { field, score: hasItems ? score : 0 };
    })
    .filter((candidate) => candidate.score > 0)
    .sort((a, b) => b.score - a.score)[0]?.field || null;
}

let fixedAssetsServerFilterSupported = null;
async function loadFixedAssetsDataset(schema, token, take, skip, groupCode = "8") {
  const queryFields = schema?.__schema?.queryType?.fields || [];
  const typeMap = new Map((schema?.__schema?.types || []).map((type) => [type.name, type]));
  const root = findStockRoot(queryFields, typeMap);
  const rootType = typeMap.get(unwrapGraphqlType(root?.type));
  const itemsField = rootType?.fields?.find((field) => field.name === "items");
  const stockType = typeMap.get(unwrapGraphqlType(itemsField?.type));
  const scalarNames = scalarFieldNames(stockType);
  if (!root || !itemsField || !scalarNames.length) {
    throw new HttpsError("failed-precondition", "A coleção Estoque não está disponível no MaxiProd.");
  }
  const selection = [...scalarNames, ...fixedAssetRelatedSelection(stockType, typeMap)].join("\n");
  const rootName = root.name;
  let data;
  let usedServerFilter = fixedAssetsServerFilterSupported !== false;
  if (usedServerFilter) {
    const filteredQuery = `query EstoquePorGrupo {
      ${rootName}(take: ${take}, skip: ${skip}, where: { item: { grupo: { codigo: { eq: "${groupCode}" } } } }) {
        items { ${selection} }
      }
    }`;
    try {
      data = await maxiprodQuery(filteredQuery, {}, token);
      fixedAssetsServerFilterSupported = true;
    } catch (error) {
      console.warn("Ativos imobilizados: filtro remoto de grupo indisponível; usando filtro local.", {
        message: error?.message || String(error),
      });
      fixedAssetsServerFilterSupported = false;
      usedServerFilter = false;
    }
  }
  if (!usedServerFilter) {
    const fallbackQuery = `query EstoquePorGrupoSemFiltro {
      ${rootName}(take: ${take}, skip: ${skip}) { items { ${selection} } }
    }`;
    data = await maxiprodQuery(fallbackQuery, {}, token);
  }
  const items = Array.isArray(data?.[rootName]?.items) ? data[rootName].items : [];
  const fixedAssets = usedServerFilter ? items : items.filter((item) => stockItemIsGroup(item, groupCode));
  const enrichedAssets = fixedAssets.map(enrichFixedAsset).filter((row) => {
    // O MaxiProd nao exibe na grade agrupada as posicoes internas totalmente
    // vazias. Ajustes com quantidade zero e custo diferente de zero continuam.
    return Math.abs(Number(row.quantidade) || 0) > 0.0000001 ||
      Math.abs(Number(row.valorEstoque) || 0) > 0.0000001;
  });
  return {
    items: enrichedAssets,
    rawCount: items.length,
    fields: scalarNames,
    rootName,
    usedServerFilter,
  };
}

export const consultarAtivosImobilizadosMaxiprod = onCall(
  {
    region: "southamerica-east1",
    timeoutSeconds: 300,
    memory: "512MiB",
    invoker: "public",
    secrets: [MAXIPROD_TOKEN],
  },
  async (request) => {
    if (!request.auth) {
      throw new HttpsError("unauthenticated", "Faça login no aplicativo para consultar os ativos imobilizados.");
    }
    const take = Math.min(Math.max(Number(request.data?.take) || 200, 20), 500);
    const skip = Math.max(Number(request.data?.skip) || 0, 0);
    try {
      const schema = await getFinanceSchema(MAXIPROD_TOKEN.value());
      const dataset = await loadFixedAssetsDataset(schema, MAXIPROD_TOKEN.value(), take, skip, "8");
      return {
        updatedAt: new Date().toISOString(),
        items: dataset.items,
        fields: dataset.fields,
        source: dataset.rootName,
        skip,
        take,
        done: dataset.rawCount < take,
        filterMode: dataset.usedServerFilter ? "maxiprod" : "local",
      };
    } catch (error) {
      console.error("consultarAtivosImobilizadosMaxiprod: falhou", {
        message: error?.message || String(error),
        stack: error?.stack || null,
      });
      if (error instanceof HttpsError) throw error;
      throw new HttpsError("internal", "Não foi possível consultar os ativos imobilizados no MaxiProd.");
    }
  },
);

export const consultarUsoConsumoMaxiprod = onCall(
  {
    region: "southamerica-east1",
    timeoutSeconds: 300,
    memory: "512MiB",
    invoker: "public",
    secrets: [MAXIPROD_TOKEN],
  },
  async (request) => {
    if (!request.auth) {
      throw new HttpsError("unauthenticated", "Faça login no aplicativo para consultar Uso e Consumo.");
    }
    const take = Math.min(Math.max(Number(request.data?.take) || 200, 20), 500);
    const skip = Math.max(Number(request.data?.skip) || 0, 0);
    try {
      const schema = await getFinanceSchema(MAXIPROD_TOKEN.value());
      const dataset = await loadFixedAssetsDataset(schema, MAXIPROD_TOKEN.value(), take, skip, "4");
      return {
        updatedAt: new Date().toISOString(),
        items: dataset.items,
        fields: dataset.fields,
        source: dataset.rootName,
        skip,
        take,
        done: dataset.rawCount < take,
        filterMode: dataset.usedServerFilter ? "maxiprod" : "local",
      };
    } catch (error) {
      console.error("consultarUsoConsumoMaxiprod: falhou", {
        message: error?.message || String(error),
        stack: error?.stack || null,
      });
      if (error instanceof HttpsError) throw error;
      throw new HttpsError("internal", "Não foi possível consultar Uso e Consumo no MaxiProd.");
    }
  },
);

// Itens da grade de Estoque: grupo 1 (matérias-primas), grupo 2
// (elementos de fixação), grupo 7 (itens comerciais) e grupo 11 (tintas). O grupo é informado
// por página para manter a paginação idêntica à grade do MaxiProd.
export const consultarMateriaPrimaMaxiprod = onCall(
  {
    region: "southamerica-east1",
    timeoutSeconds: 300,
    memory: "512MiB",
    invoker: "public",
    secrets: [MAXIPROD_TOKEN],
  },
  async (request) => {
    if (!request.auth) {
      throw new HttpsError("unauthenticated", "Faça login no aplicativo para consultar o Estoque.");
    }
    const groupCode = String(request.data?.groupCode ?? "").trim();
    if (!["1", "2", "7", "11"].includes(groupCode)) {
      throw new HttpsError("invalid-argument", "Informe um dos grupos de estoque: 1, 2, 7 ou 11.");
    }
    const take = Math.min(Math.max(Number(request.data?.take) || 200, 20), 500);
    const skip = Math.max(Number(request.data?.skip) || 0, 0);
    try {
      const schema = await getFinanceSchema(MAXIPROD_TOKEN.value());
      const dataset = await loadFixedAssetsDataset(schema, MAXIPROD_TOKEN.value(), take, skip, groupCode);
      return {
        updatedAt: new Date().toISOString(),
        items: dataset.items.map((item) => ({ ...item, grupo: groupCode })),
        fields: dataset.fields,
        source: dataset.rootName,
        groupCode,
        skip,
        take,
        done: dataset.rawCount < take,
        filterMode: dataset.usedServerFilter ? "maxiprod" : "local",
      };
    } catch (error) {
      console.error("consultarMateriaPrimaMaxiprod: falhou", {
        groupCode,
        message: error?.message || String(error),
        stack: error?.stack || null,
      });
      if (error instanceof HttpsError) throw error;
      throw new HttpsError("internal", `Não foi possível consultar o grupo ${groupCode} do Estoque no MaxiProd.`);
    }
  },
);

// O MaxiProd trabalha de forma estável com páginas de até 200 registros. As
// consultas antigas do portal já utilizavam esse tamanho; pedir 500 fazia a
// primeira atualização central falhar antes de publicar o manifesto.
async function loadEveryPage(loadPage, pageSize = 200, maxPages = 625) {
  const items = [];
  let lastDataset = null;
  for (let page = 0; page < maxPages; page++) {
    const skip = page * pageSize;
    const dataset = await loadPage(pageSize, skip);
    lastDataset = dataset;
    const pageItems = Array.isArray(dataset?.items) ? dataset.items : [];
    items.push(...pageItems);
    const rawCount = Number.isFinite(Number(dataset?.rawCount))
      ? Number(dataset.rawCount)
      : pageItems.length;
    if (rawCount < pageSize) break;
    if (page === maxPages - 1) {
      throw new Error(`A consulta ultrapassou o limite de ${maxPages * pageSize} registros.`);
    }
  }
  return { items, dataset: lastDataset };
}

async function loadSharedStage(label, task) {
  try {
    return await task();
  } catch (error) {
    console.error(`Cache compartilhado: falha em ${label}.`, {
      message: error?.message || String(error),
      stack: error?.stack || null,
    });
    throw new HttpsError(
      "internal",
      `Não foi possível carregar ${label} no cache compartilhado do MaxiProd.`,
    );
  }
}

function uniqueSharedRows(items) {
  const rows = [];
  const seen = new Set();
  for (const item of Array.isArray(items) ? items : []) {
    const id = item?.id ?? item?.pedidoDeVendaId ?? item?.pedidoDeCompraId ?? null;
    const key = id === null || id === undefined || id === ""
      ? null
      : String(id);
    if (key && seen.has(key)) continue;
    if (key) seen.add(key);
    rows.push(item);
  }
  return rows;
}

function sharedFinanceRecordYear(item) {
  const value = findFinanceValue(item, ["vencimentoData", "vencimento", "vencimentoOriginalData", "vencimentoOriginal"]);
  const text = String(value || "").trim();
  const iso = text.match(/^(\d{4})-\d{2}-\d{2}/);
  if (iso) return Number(iso[1]);
  const br = text.match(/^\d{2}\/\d{2}\/(\d{4})/);
  if (br) return Number(br[1]);
  const parsed = new Date(text);
  return Number.isNaN(parsed.getTime()) ? null : parsed.getFullYear();
}

async function buildSharedMaxiprodCache() {
  const token = MAXIPROD_TOKEN.value();
  const schema = await loadSharedStage("a estrutura da API", () => getFinanceSchema(token));
  const currentYear = new Date().getFullYear();
  const dateFrom = `${currentYear}-01-01`;
  const dateTo = `${currentYear + 1}-01-01`;
  // A Contabilidade precisa mostrar a condição completa do pedido, inclusive
  // parcelas antigas recebidas/pagas e vencimentos futuros. O intervalo fica
  // isolado do Financeiro/Fiscal, que continua recebendo somente o ano atual.
  const accountingDateFrom = "2020-01-01";
  const accountingDateTo = `${currentYear + 7}-01-01`;
  const updatedAt = new Date().toISOString();

  // As consultas são intencionalmente sequenciais para não sobrecarregar a API
  // do MaxiProd. Todos os navegadores passam a consumir este único resultado.
  const accountingReceivable = await loadSharedStage("Contabilidade — histórico a receber", () =>
    loadEveryPage((take, skip) =>
      loadFinanceDataset("contaAReceber", schema, token, take, skip, accountingDateFrom, accountingDateTo, true)));
  const accountingPayable = await loadSharedStage("Contabilidade — histórico a pagar", () =>
    loadEveryPage((take, skip) =>
      loadFinanceDataset("contaAPagar", schema, token, take, skip, accountingDateFrom, accountingDateTo, true)));
  const sales = await loadSharedStage("Pedidos de venda", () =>
    loadEveryPage((take, skip) =>
      loadSalesOrdersDataset(schema, token, take, skip)));
  const purchaseOrders = await loadSharedStage("Pedidos de compra", () =>
    loadEveryPage((take, skip) =>
      loadPurchaseOrdersDataset(schema, token, take, skip)));
  const fixedAssets = await loadSharedStage("Estoque — grupo 8", () =>
    loadEveryPage((take, skip) =>
      loadFixedAssetsDataset(schema, token, take, skip, "8")));
  const usageStock = await loadSharedStage("Estoque — grupo 4", () =>
    loadEveryPage((take, skip) =>
      loadFixedAssetsDataset(schema, token, take, skip, "4")));

  const rawMaterialItems = [];
  for (const groupCode of ["1", "2", "7", "11"]) {
    const group = await loadSharedStage(`Estoque — grupo ${groupCode}`, () =>
      loadEveryPage((take, skip) =>
        loadFixedAssetsDataset(schema, token, take, skip, groupCode)));
    rawMaterialItems.push(...group.items.map((item) => ({ ...item, grupo: groupCode })));
  }

  const contabilidadeReceber = uniqueSharedRows(accountingReceivable.items);
  const contabilidadePagar = uniqueSharedRows(accountingPayable.items);
  const contaAReceber = contabilidadeReceber.filter((item) =>
    sharedFinanceRecordYear(item) === currentYear && receivableIsAllowed(item));
  const contaAPagar = contabilidadePagar.filter((item) =>
    sharedFinanceRecordYear(item) === currentYear && payableIsAllowed(item));
  const pedidosVenda = uniqueSharedRows(sales.items);
  const pedidosCompra = uniqueSharedRows(purchaseOrders.items);
  const ativos = uniqueSharedRows(fixedAssets.items);
  const usoConsumo = uniqueSharedRows(usageStock.items);
  const materiaPrima = uniqueSharedRows(rawMaterialItems);
  const payload = {
    version: 1,
    updatedAt,
    currentYear,
    finance: {
      version: 55,
      updatedAt,
      datasets: {
        contaAReceber: { items: contaAReceber, available: true },
        contaAPagar: { items: contaAPagar, available: true },
        pedidosDeVenda: { items: pedidosVenda, available: true },
      },
      payableLoadedYears: [currentYear],
      receivableLoadedYears: [currentYear],
      payableYearCacheVersion: 1,
      receivableYearCacheVersion: 1,
      payableCurrentYear: currentYear,
      receivableCurrentYear: currentYear,
    },
    sales: { version: 8, updatedAt, items: pedidosVenda },
    purchaseOrders: { version: 1, updatedAt, items: pedidosCompra },
    accounting: {
      version: 1,
      updatedAt,
      dateFrom: accountingDateFrom,
      dateTo: accountingDateTo,
      receivable: { items: contabilidadeReceber, available: true },
      payable: { items: contabilidadePagar, available: true },
    },
    fixedAssets: { version: 9, updatedAt, items: ativos },
    usageStock: { version: 2, updatedAt, items: usoConsumo },
    rawMaterial: { version: 1, updatedAt, items: materiaPrima },
    stats: {
      contaAReceber: contaAReceber.length,
      contaAPagar: contaAPagar.length,
      vendas: pedidosVenda.length,
      pedidosCompra: pedidosCompra.length,
      contabilidadeReceber: contabilidadeReceber.length,
      contabilidadePagar: contabilidadePagar.length,
      ativosImobilizados: ativos.length,
      usoConsumo: usoConsumo.length,
      estoqueGrupos: materiaPrima.length,
    },
  };
  return loadSharedStage("a gravação central no Firebase", () =>
    saveSharedMaxiprodCache(getFirestore(), payload));
}

export const atualizarCacheCompartilhadoMaxiprod = onCall(
  {
    region: "southamerica-east1",
    timeoutSeconds: 1800,
    memory: "1GiB",
    invoker: "public",
    secrets: [MAXIPROD_TOKEN],
  },
  async (request) => {
    if (!request.auth) {
      throw new HttpsError("unauthenticated", "Faça login no aplicativo para atualizar o MaxiProd.");
    }
    try {
      return await buildSharedMaxiprodCache();
    } catch (error) {
      console.error("atualizarCacheCompartilhadoMaxiprod: falhou", {
        message: error?.message || String(error),
        stack: error?.stack || null,
      });
      if (error instanceof HttpsError) throw error;
      throw new HttpsError("internal", "Não foi possível atualizar o cache compartilhado do MaxiProd.");
    }
  },
);

function pushNorm(value) {
  return String(value || "").normalize("NFD").replace(/[\u0300-\u036f]/g, "").trim().toLowerCase().replace(/\s+/g, " ");
}

function pushHash(value) {
  let hash = 2166136261;
  for (let index = 0; index < value.length; index += 1) {
    hash ^= value.charCodeAt(index);
    hash = Math.imul(hash, 16777619);
  }
  return `ntf_${(hash >>> 0).toString(36)}`;
}

function pushToday() {
  return new Intl.DateTimeFormat("en-CA", { timeZone: "America/Sao_Paulo", year: "numeric", month: "2-digit", day: "2-digit" }).format(new Date());
}

function pushDate(value) {
  const match = /^(\d{4})-(\d{2})-(\d{2})/.exec(String(value || ""));
  return match ? `${match[1]}-${match[2]}-${match[3]}` : "";
}

function pushWorkdayDiff(fromIso, toIso) {
  if (!fromIso || !toIso || fromIso === toIso) return 0;
  const direction = fromIso < toIso ? 1 : -1;
  let cursor = new Date(`${fromIso}T12:00:00Z`);
  const target = new Date(`${toIso}T12:00:00Z`);
  let total = 0;
  while ((direction > 0 && cursor < target) || (direction < 0 && cursor > target)) {
    cursor.setUTCDate(cursor.getUTCDate() + direction);
    if (cursor.getUTCDay() !== 0 && cursor.getUTCDay() !== 6) total += direction;
  }
  return total;
}

function pushTasks(order) {
  return (order?.depts || []).flatMap((department) => (department?.tasks || []).map((task) => ({ task, department })));
}

async function generateCronogramaNotifications() {
  const database = getFirestore();
  const [dbSnap, notificationsSnap] = await Promise.all([
    database.collection("cartamac").doc("db").get(),
    database.collection("cartamac").doc("notifications").get(),
  ]);
  const appData = dbSnap.data() || {};
  const settings = { enabled: true, daysBefore: 2, notifyStart: true, notifyDueSoon: true, notifyOverdue: true, ...(appData.notificationSettings || {}) };
  if (!settings.enabled) return { created: 0, reason: "disabled" };
  const orders = Array.isArray(appData.projectOrders) ? appData.projectOrders : [];
  const admins = Array.isArray(appData.admins) ? appData.admins : [];
  const items = Array.isArray(notificationsSnap.data()?.items) ? notificationsSnap.data().items : [];
  const existing = new Set(items.map((item) => item.id));
  const today = pushToday();
  let created = 0;

  const add = (recipient, type, order, task, title, message, eventKey) => {
    const id = pushHash([recipient.id, type, order?.id ?? "", task?.sourceId || task?.name || "", eventKey].join("|"));
    if (existing.has(id)) return;
    items.push({ id, userId: recipient.id, type, title, message, orderId: order?.id ?? null, taskId: task?.sourceId || null, createdAt: new Date().toISOString(), readAt: null, pushStatus: "pending" });
    existing.add(id);
    created += 1;
  };

  for (const order of orders) {
    for (const { task } of pushTasks(order)) {
      if (pushDate(task.actualFinish)) continue;
      const operator = pushNorm(task.operator);
      if (!operator) continue;
      const recipients = admins.filter((admin) => admin?.notificationsEnabled !== false && pushNorm(admin?.name) === operator);
      if (!recipients.length) continue;
      const start = pushDate(task.actualStart || task.baselineStart || task.start);
      const finish = pushDate(task.baselineFinish || task.finish);
      const daysToStart = start ? pushWorkdayDiff(today, start) : null;
      const daysToFinish = finish ? pushWorkdayDiff(today, finish) : null;
      const overdueDays = finish && finish < today ? Math.max(1, pushWorkdayDiff(finish, today)) : 0;
      for (const recipient of recipients) {
        if (settings.notifyStart && daysToStart === 0) add(recipient, "start", order, task, "Operação começa hoje", `${order.code || order.op || "Pedido"} · ${task.name}`, `start:${start}`);
        if (settings.notifyDueSoon && daysToFinish !== null && daysToFinish >= 0 && daysToFinish <= Number(settings.daysBefore || 0)) add(recipient, "due", order, task, "Operação próxima do vencimento", `${order.code || order.op || "Pedido"} · ${task.name} · término ${finish}`, `due:${finish}`);
        if (settings.notifyOverdue && overdueDays > 0) add(recipient, "overdue", order, task, "Operação atrasada", `${order.code || order.op || "Pedido"} · ${task.name} · ${overdueDays} dia(s) útil(eis)`, `overdue:${today}`);
      }
    }
  }

  if (created) {
    items.sort((a, b) => String(b.createdAt || "").localeCompare(String(a.createdAt || "")));
    await database.collection("cartamac").doc("notifications").set({ items: items.slice(0, 700), updatedAt: new Date().toISOString(), generatedBy: "scheduled-maxiprod" });
  }
  return { created, orders: orders.length };
}

export const enviarPushNotificacoesCronograma = onDocumentWritten(
  { document: "cartamac/notifications", region: "southamerica-east1", memory: "256MiB", timeoutSeconds: 120 },
  async (event) => {
    const beforeItems = Array.isArray(event.data?.before.data()?.items) ? event.data.before.data().items : [];
    const afterItems = Array.isArray(event.data?.after.data()?.items) ? event.data.after.data().items : [];
    const previousIds = new Set(beforeItems.map((item) => item.id));
    const newItems = afterItems.filter((item) => item?.id && !previousIds.has(item.id));
    if (!newItems.length) return;

    const database = getFirestore();
    const [devicesSnap, dbSnap] = await Promise.all([
      database.collection("cartamac").doc("push-devices").get(),
      database.collection("cartamac").doc("db").get(),
    ]);
    let devices = Array.isArray(devicesSnap.data()?.devices) ? devicesSnap.data().devices : [];
    const orders = Array.isArray(dbSnap.data()?.projectOrders) ? dbSnap.data().projectOrders : [];
    const invalidTokens = new Set();
    const appUrl = "https://cartamac.github.io/Cronograma/";

    const itemStillOpen = (item) => {
      if (item.type === "test") return true;
      const order = orders.find((row) => String(row.id) === String(item.orderId));
      if (!order) return false;
      const task = pushTasks(order).map((entry) => entry.task).find((row) => String(row.sourceId || "") === String(item.taskId || ""));
      return Boolean(task) && !pushDate(task.actualFinish);
    };

    for (const item of newItems.filter(itemStillOpen)) {
      const tokens = [...new Set(devices.filter((device) => device?.enabled !== false && String(device.userId) === String(item.userId)).map((device) => device.token).filter(Boolean))];
      for (let offset = 0; offset < tokens.length; offset += 500) {
        const batch = tokens.slice(offset, offset + 500);
        const response = await getMessaging().sendEachForMulticast({
          tokens: batch,
          data: { title: String(item.title || "CARTAMAC"), body: String(item.message || "Nova atualização no cronograma."), url: appUrl, notificationId: String(item.id) },
          webpush: {
            headers: { Urgency: item.type === "overdue" ? "high" : "normal" },
            notification: {
              title: String(item.title || "CARTAMAC"),
              body: String(item.message || "Nova atualização no cronograma."),
              icon: `${appUrl}cartamac-logo.png`,
              badge: `${appUrl}cartamac-logo.png`,
              tag: String(item.id),
              renotify: true,
            },
            fcmOptions: { link: appUrl },
          },
        });
        console.log("Resultado do push do cronograma.", {
          notificationId: item.id,
          userId: item.userId,
          devices: batch.length,
          successCount: response.successCount,
          failureCount: response.failureCount,
          errors: response.responses.filter((result) => result.error).map((result) => result.error?.code || result.error?.message || "erro-desconhecido"),
        });
        response.responses.forEach((result, index) => {
          const code = result.error?.code || "";
          if (code.includes("registration-token-not-registered") || code.includes("invalid-registration-token")) invalidTokens.add(batch[index]);
        });
      }
    }

    if (invalidTokens.size) {
      devices = devices.filter((device) => !invalidTokens.has(device.token));
      await database.collection("cartamac").doc("push-devices").set({ devices, updatedAt: new Date().toISOString() });
    }
  },
);

export const atualizarCacheCompartilhadoMaxiprodAutomaticamente = onSchedule(
  {
    region: "southamerica-east1",
    schedule: "0 */2 * * *",
    timeZone: "America/Sao_Paulo",
    timeoutSeconds: 1800,
    memory: "1GiB",
    secrets: [MAXIPROD_TOKEN],
  },
  async () => {
    try {
      const result = await buildSharedMaxiprodCache();
      console.log("Cache compartilhado do MaxiProd atualizado automaticamente.", result);
      const notificationResult = await generateCronogramaNotifications();
      console.log("Notificações do cronograma verificadas junto com o ciclo de 2 horas.", notificationResult);
    } catch (error) {
      console.error("Cache compartilhado do MaxiProd: atualização automática falhou.", {
        message: error?.message || String(error),
        stack: error?.stack || null,
      });
      throw error;
    }
  },
);
