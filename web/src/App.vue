<template>
  <div class="bg-decor"><span class="glow blue"></span><span class="glow red"></span></div>

  <SideBar />

  <div class="main">
    <ControlsBar @open-skills="skillHubOpen = true" />
    <MessageList />
    <ChatInput />
  </div>

  <AuthModal v-if="auth.overlay" />
  <SkillHub v-if="skillHubOpen" @close="skillHubOpen = false" />
  <DocViewer />
</template>

<script setup>
import { ref, watch, onMounted, onBeforeUnmount } from 'vue'
import { setUnauthorizedHandler } from './api/client'
import { useAuth } from './stores/auth'
import { useDocs } from './stores/docs'
import { useSkills } from './stores/skills'
import { useSessions } from './stores/sessions'
import { useChat } from './stores/chat'
import SideBar from './components/SideBar.vue'
import ControlsBar from './components/ControlsBar.vue'
import MessageList from './components/MessageList.vue'
import ChatInput from './components/ChatInput.vue'
import AuthModal from './components/AuthModal.vue'
import SkillHub from './components/SkillHub.vue'
import DocViewer from './components/DocViewer.vue'

const auth = useAuth()
const docs = useDocs()
const skills = useSkills()
const sessions = useSessions()
const chat = useChat()
const skillHubOpen = ref(false)

// 401 → 清 token + 弹登录层 + 中断在飞的对话
setUnauthorizedHandler(() => {
  chat.abortInflight()
  auth.logout()
})

// 只有服务端确认的身份才能恢复本地会话；切换身份同步清理可见状态。
watch(() => auth.userId, (id) => {
  chat.abortInflight()
  sessions.setUser(id)
  docs.reset()
  skills.reset()
  skillHubOpen.value = false
  if (id) { docs.loadLibrary(); skills.loadInstalled() }
}, { immediate: true, flush: 'sync' })

onMounted(() => auth.restore())
onBeforeUnmount(() => { chat.abortInflight(); setUnauthorizedHandler(null) })
</script>
