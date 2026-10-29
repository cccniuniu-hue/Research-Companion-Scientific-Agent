<template>
  <div v-if="hasThink" class="think-wrap">
    <button type="button" class="think-header" :class="{ open }" :aria-expanded="open" @click="open = !open">
      <span class="think-icon">▶</span>
      <span class="think-label">执行过程</span>
      <span class="think-badge" :class="badgeClass">{{ badgeText }}</span>
    </button>
    <div class="think-body" :class="{ open }">
      <div v-if="msg.sandbox" class="step-row">
        <span class="step-type step-action">📦 沙箱</span>
        <div class="step-content">服务端工作目录：{{ msg.sandbox }}</div>
      </div>

      <div v-for="(s, i) in steps" :key="i" class="step-row">
        <span class="step-type" :class="stepCls(s.type)">{{ stepLabel(s.type) }}</span>
        <div class="step-content">
          {{ s.content }}
          <div v-if="s.params && Object.keys(s.params).length" class="step-params">{{ JSON.stringify(s.params) }}</div>
        </div>
      </div>

      <div v-if="msg.toolCall" class="tool-call-block">
        <div class="tool-call-row"><span class="tool-call-label">工具</span><span class="tool-call-val">{{ msg.toolCall.tool_name }}</span></div>
        <div class="tool-call-row"><span class="tool-call-label">参数</span><span class="tool-call-val" style="font-family:monospace">{{ JSON.stringify(msg.toolCall.params || {}) }}</span></div>
        <div class="tool-call-row"><span class="tool-call-label">结果</span><span class="tool-call-val tool-call-result">{{ msg.toolCall.tool_result || '' }}</span></div>
      </div>

      <div v-if="results.length" class="rag-sources">
        <div v-for="(r, i) in results.slice(0, 3)" :key="i" class="rag-chunk">
          <span class="rag-chunk-score">{{ score(r) }}</span>{{ preview(r) }}
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, computed } from 'vue'

const props = defineProps({ msg: { type: Object, required: true } })
const open = ref(true)
const records = value => Array.isArray(value) ? value.filter(item => item && typeof item === 'object' && !Array.isArray(item)) : []
const steps = computed(() => records(props.msg.steps))
const results = computed(() => records(props.msg.ragResults))

const hasThink = computed(() => {
  const m = props.msg
  return steps.value.length || m.toolCall || results.value.length || m.sandbox
})

const LABELS = { react: '多步执行', rag_agent: 'Agentic RAG', tool: '工具调用', rag: '知识检索', chat: '对话' }
const CLASSES = { react: 'badge-react', rag_agent: 'badge-rag', tool: 'badge-tool', rag: 'badge-rag', chat: 'badge-react' }
const badgeText = computed(() => (props.msg.streaming && !props.msg.mode) ? '处理中' : (LABELS[props.msg.mode] || props.msg.mode || '执行记录'))
const badgeClass = computed(() => CLASSES[props.msg.mode] || 'badge-react')

const STEP = {
  Thought: ['step-thought', '💭 思考'],
  Action: ['step-action', '⚡ 动作'],
  Observation: ['step-observation', '🔍 观察'],
  'Final Answer': ['step-final', '✓ 汇总'],
}
function stepCls(t) { return (STEP[t] || ['step-thought'])[0] }
function stepLabel(t) { return (STEP[t] || [null, t])[1] }
function score(r) {
  return typeof r.similarity === 'number' && Number.isFinite(r.similarity)
    ? `相关度 ${(r.similarity * 100).toFixed(1)}%`
    : '未提供相关度'
}
function preview(r) {
  const c = typeof r.chunk?.content === 'string' ? r.chunk.content : ''
  return c.slice(0, 120) + (c.length > 120 ? '…' : '')
}
</script>

<style scoped>
.think-header { width: 100%; border: 0; background: transparent; font: inherit; color: inherit; text-align: left; }
.think-header:focus-visible { outline: 2px solid var(--primary-d); outline-offset: -2px; }
.step-content, .tool-call-val, .rag-chunk { min-width: 0; overflow-wrap: anywhere; }
.step-params { white-space: pre-wrap; overflow-wrap: anywhere; }
</style>
