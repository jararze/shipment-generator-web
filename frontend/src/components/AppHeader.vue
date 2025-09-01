<script>
import { ref, onMounted } from 'vue'
import { useShipmentApi } from '@/composables/useApi'
import {
  DocumentTextIcon,
  ShieldCheckIcon,
  ArrowPathIcon
} from '@heroicons/vue/24/outline'

export default {
  name: 'AppHeader',
  components: {
    DocumentTextIcon,
    ShieldCheckIcon,
    ArrowPathIcon
  },
  setup() {
    const { healthCheck } = useShipmentApi()
    const connectionStatus = ref('checking')
    const isCheckingHealth = ref(false)

    const checkHealth = async () => {
      try {
        isCheckingHealth.value = true
        await healthCheck()
        connectionStatus.value = 'connected'
      } catch (error) {
        connectionStatus.value = 'disconnected'
        console.error('Health check failed:', error)
      } finally {
        isCheckingHealth.value = false
      }
    }

    // Verificar conexión al cargar
    onMounted(() => {
      checkHealth()

      // Verificar cada 30 segundos
      setInterval(checkHealth, 30000)
    })

    return {
      connectionStatus,
      isCheckingHealth,
      checkHealth
    }
  }
}
</script>

<template>
  <header class="bg-gradient-to-r from-blue-600 to-blue-800 text-white shadow-lg">
    <div class="max-w-6xl mx-auto px-4 py-6">
      <div class="flex items-center justify-between">
        <div class="flex items-center space-x-3">
          <DocumentTextIcon class="w-8 h-8" />
          <div>
            <h1 class="text-3xl font-bold">Shipment XML Generator v5.0</h1>
            <p class="text-blue-100 mt-1">Generador XML para TMS con integración de base de datos</p>
          </div>
        </div>

        <!-- Indicador de conexión -->
        <div class="flex items-center space-x-4">
          <div class="flex items-center space-x-2">
            <div
              class="w-3 h-3 rounded-full"
              :class="connectionStatus === 'connected' ? 'bg-green-400' : 'bg-red-400'"
            ></div>
            <span class="text-sm text-blue-100">
              {{ connectionStatus === 'connected' ? 'Conectado' : 'Desconectado' }}
            </span>
          </div>

          <!-- Botón de health check -->
          <button
            @click="checkHealth"
            :disabled="isCheckingHealth"
            class="bg-blue-700 hover:bg-blue-600 px-3 py-2 rounded-lg text-sm
                   transition-colors disabled:opacity-50"
            title="Verificar estado del servidor"
          >
            <component
              :is="isCheckingHealth ? 'ArrowPathIcon' : 'ShieldCheckIcon'"
              class="w-4 h-4"
              :class="{ 'animate-spin': isCheckingHealth }"
            />
          </button>
        </div>
      </div>
    </div>
  </header>
</template>

<style scoped>

</style>
