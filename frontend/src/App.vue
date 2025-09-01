<!-- App.vue - Vue 3 Frontend Principal -->
<script>
import { ref, reactive, onMounted, computed } from 'vue'
import { useShipmentApi } from './composables/useApi'

// Componentes
import AppHeader from './components/AppHeader.vue'
import FileUpload from './components/FileUpload.vue'
import AvailabilityUpload from './components/AvailabilityUpload.vue'
import ProcessingOptions from './components/ProcessingOptions.vue'
import ProcessingStatus from './components/ProcessingStatus.vue'
import ResultsDisplay from './components/ResultsDisplay.vue'

// Iconos
import {
  PlayIcon,
  CircleStackIcon,
  CheckIcon,
  CalculatorIcon,
  ClockIcon,
  CheckCircleIcon,
  ExclamationTriangleIcon,
  XMarkIcon,
  InformationCircleIcon
} from '@heroicons/vue/24/outline'

export default {
  name: 'ShipmentGenerator',
  components: {
    AppHeader,
    FileUpload,
    AvailabilityUpload,
    ProcessingOptions,
    ProcessingStatus,
    ResultsDisplay,
    PlayIcon,
    CircleStackIcon,
    CheckIcon,
    CalculatorIcon,
    ClockIcon,
    CheckCircleIcon,
    ExclamationTriangleIcon,
    XMarkIcon,
    InformationCircleIcon
  },
  setup() {
    // Composables
    const { uploadFile, pollJobStatus, jobs } = useShipmentApi()

    // Estado local
    const selectedFile = ref(null)
    const availabilityFile = ref(null);
    const isProcessing = ref(false)
    const currentJobId = ref(null)
    const notifications = ref([])

    const processingOptions = reactive({
      usePlantaAsOrigen: false,
      skipPlacas: false
    })

    const handleAvailabilityFileSelected = (file) => {
      availabilityFile.value = file;
      addNotification('info', 'Archivo de disponibilidad seleccionado');
    };

    const handleAvailabilityFileCleared = () => {
      availabilityFile.value = null
    }

    // Computed properties
    const currentJob = computed(() => {
      return currentJobId.value ? jobs.get(currentJobId.value) : null
    })

    const recentJobs = computed(() => {
      return Array.from(jobs.values())
        .sort((a, b) => new Date(b.started_at) - new Date(a.started_at))
        .slice(0, 10)
    })

    // Event handlers
    const handleFileSelected = (file) => {
      selectedFile.value = file
      addNotification('info', 'Archivo seleccionado', `${file.name} listo para procesar`)
    }

    const handleFileCleared = () => {
      selectedFile.value = null
    }

    const handleOptionsChanged = (newOptions) => {
      Object.assign(processingOptions, newOptions)
    }

    const processFile = async () => {
      if (!selectedFile.value) return

      try {
        isProcessing.value = true

        addNotification('info', 'Iniciando procesamiento', 'Subiendo archivo al servidor...')

        const result = await uploadFile(selectedFile.value, availabilityFile.value, processingOptions);
        currentJobId.value = result.job_id

        addNotification('success', 'Archivo subido', 'Procesamiento iniciado correctamente')

        // Iniciar polling
        pollJobStatus(result.job_id)

      } catch (error) {
        addNotification('error', 'Error de procesamiento', error.message)
        console.error('Error procesando archivo:', error)
      } finally {
        isProcessing.value = false
      }
    }

    const loadJob = (jobId) => {
      currentJobId.value = jobId
    }

    // Utilidades
    const getJobStatusIcon = (job) => {
      switch (job.status) {
        case 'completed': return 'CheckCircleIcon'
        case 'error': return 'ExclamationTriangleIcon'
        case 'processing': return 'ClockIcon'
        default: return 'InformationCircleIcon'
      }
    }

    const getJobStatusColor = (job) => {
      switch (job.status) {
        case 'completed': return 'text-green-500'
        case 'error': return 'text-red-500'
        case 'processing': return 'text-blue-500'
        default: return 'text-gray-500'
      }
    }

    const formatTimeAgo = (dateString) => {
      if (!dateString) return 'Hace un momento'

      const now = new Date()
      const date = new Date(dateString)
      const diffMs = now - date
      const diffMins = Math.floor(diffMs / 60000)

      if (diffMins < 1) return 'Hace un momento'
      if (diffMins < 60) return `Hace ${diffMins}m`

      const diffHours = Math.floor(diffMins / 60)
      if (diffHours < 24) return `Hace ${diffHours}h`

      const diffDays = Math.floor(diffHours / 24)
      return `Hace ${diffDays}d`
    }

    // Sistema de notificaciones
    let notificationId = 0

    const addNotification = (type, title, message = '') => {
      const id = ++notificationId

      notifications.value.push({
        id,
        type,
        title,
        message
      })

      // Auto-remove después de 5 segundos
      setTimeout(() => {
        removeNotification(id)
      }, 5000)
    }

    const removeNotification = (id) => {
      const index = notifications.value.findIndex(n => n.id === id)
      if (index > -1) {
        notifications.value.splice(index, 1)
      }
    }

    const getNotificationClass = (type) => {
      const classes = {
        success: 'border-green-200 bg-green-50',
        error: 'border-red-200 bg-red-50',
        warning: 'border-yellow-200 bg-yellow-50',
        info: 'border-blue-200 bg-blue-50'
      }
      return classes[type] || classes.info
    }

    const getNotificationIcon = (type) => {
      const icons = {
        success: 'CheckCircleIcon',
        error: 'ExclamationTriangleIcon',
        warning: 'ExclamationTriangleIcon',
        info: 'InformationCircleIcon'
      }
      return icons[type] || icons.info
    }

    const getNotificationIconColor = (type) => {
      const colors = {
        success: 'text-green-500',
        error: 'text-red-500',
        warning: 'text-yellow-500',
        info: 'text-blue-500'
      }
      return colors[type] || colors.info
    }

    // Lifecycle
    onMounted(() => {
      console.log('🚀 Shipment Generator iniciado')
    })

    return {
      // Estado
      selectedFile,
      isProcessing,
      currentJob,
      recentJobs,
      processingOptions,
      notifications,

      // Métodos
      handleFileSelected,
      handleFileCleared,
      handleAvailabilityFileSelected,
      handleAvailabilityFileCleared,
      handleOptionsChanged,
      processFile,
      loadJob,
      getJobStatusIcon,
      getJobStatusColor,
      formatTimeAgo,
      removeNotification,
      getNotificationClass,
      getNotificationIcon,
      getNotificationIconColor
    }
  }
}
</script>

<template>
  <div id="app" class="min-h-screen bg-gray-50">
    <!-- Header -->
    <AppHeader />

    <!-- Main Content -->
    <main class="max-w-6xl mx-auto px-4 py-8">

      <!-- File Upload Section -->
      <FileUpload
        @file-selected="handleFileSelected"
        @file-cleared="handleFileCleared"
      />

      <AvailabilityUpload
        @file-selected="handleAvailabilityFileSelected"
        @file-cleared="handleAvailabilityFileCleared"
      />

      <!-- Options Section -->
      <ProcessingOptions
        :options="processingOptions"
        @options-changed="handleOptionsChanged"
      />

      <!-- Process Button -->
      <section class="mb-6">
        <button
          @click="processFile"
          :disabled="!selectedFile || isProcessing"
          class="w-full bg-green-600 text-white py-4 px-6 rounded-lg font-semibold text-lg
                 hover:bg-green-700 disabled:bg-gray-400 disabled:cursor-not-allowed
                 transition-colors flex items-center justify-center space-x-2 shadow-lg"
        >
          <PlayIcon v-if="!isProcessing" class="w-6 h-6" />
          <div v-else class="w-6 h-6 border-2 border-white border-t-transparent rounded-full animate-spin"></div>
          <span>{{ isProcessing ? 'Procesando...' : 'Generar XML' }}</span>
        </button>
      </section>

      <!-- Processing Status -->
      <ProcessingStatus :job="currentJob" />

      <!-- Results Display -->
      <ResultsDisplay :job="currentJob" />

      <!-- Database Info Section -->
      <section class="bg-white rounded-lg shadow-md p-6 mb-6">
        <h2 class="text-xl font-semibold mb-4 flex items-center">
          <CircleStackIcon class="w-5 h-5 mr-2" />
          Características v5.0
        </h2>

        <div class="grid md:grid-cols-2 gap-6">
          <div>
            <h3 class="font-semibold text-blue-700 mb-3 flex items-center">
              <CircleStackIcon class="w-4 h-4 mr-2" />
              Integración Base de Datos
            </h3>
            <ul class="space-y-2 text-sm text-gray-600">
              <li class="flex items-center space-x-2">
                <CheckIcon class="w-4 h-4 text-green-500" />
                <span>SKU Names desde dados_produtos</span>
              </li>
              <li class="flex items-center space-x-2">
                <CheckIcon class="w-4 h-4 text-green-500" />
                <span>Prioridades desde maestro_dtto</span>
              </li>
              <li class="flex items-center space-x-2">
                <CheckIcon class="w-4 h-4 text-green-500" />
                <span>Commodities desde BD</span>
              </li>
            </ul>
          </div>

          <div>
            <h3 class="font-semibold text-green-700 mb-3 flex items-center">
              <CalculatorIcon class="w-4 h-4 mr-2" />
              Cálculos Automáticos
            </h3>
            <ul class="space-y-2 text-sm text-gray-600">
              <li class="flex items-center space-x-2">
                <CheckIcon class="w-4 h-4 text-green-500" />
                <span>Hectolitros calculados</span>
              </li>
              <li class="flex items-center space-x-2">
                <CheckIcon class="w-4 h-4 text-green-500" />
                <span>Bultos calculados</span>
              </li>
              <li class="flex items-center space-x-2">
                <CheckIcon class="w-4 h-4 text-green-500" />
                <span>ReferenceNumber correlativo</span>
              </li>
            </ul>
          </div>
        </div>
      </section>

      <!-- Recent Jobs (opcional) -->
      <section v-if="recentJobs.length > 0" class="bg-white rounded-lg shadow-md p-6">
        <h2 class="text-xl font-semibold mb-4 flex items-center">
          <ClockIcon class="w-5 h-5 mr-2" />
          Trabajos Recientes
        </h2>

        <div class="space-y-2">
          <div
            v-for="job in recentJobs.slice(0, 5)"
            :key="job.job_id"
            class="flex items-center justify-between p-3 bg-gray-50 rounded-lg hover:bg-gray-100
                   transition-colors cursor-pointer"
            @click="loadJob(job.job_id)"
          >
            <div class="flex items-center space-x-3">
              <component
                :is="getJobStatusIcon(job)"
                class="w-4 h-4"
                :class="getJobStatusColor(job)"
              />
              <span class="font-medium">{{ job.filename || `Job ${job.job_id.slice(0, 8)}` }}</span>
            </div>
            <div class="text-sm text-gray-500">
              {{ formatTimeAgo(job.started_at) }}
            </div>
          </div>
        </div>
      </section>
    </main>

    <!-- Footer -->
    <footer class="bg-gray-800 text-white py-6 mt-12">
      <div class="max-w-6xl mx-auto px-4">
        <div class="grid md:grid-cols-3 gap-6">
          <div>
            <h3 class="font-semibold mb-2">Shipment XML Generator v5.0</h3>
            <p class="text-gray-300 text-sm">
              Generador XML para TMS con integración completa de base de datos.
            </p>
          </div>
          <div>
            <h3 class="font-semibold mb-2">Soporte</h3>
            <ul class="text-gray-300 text-sm space-y-1">
              <li>• Archivos Excel (.xlsx, .xlsm, .xls)</li>
              <li>• Integración MySQL</li>
              <li>• Cálculos automáticos</li>
            </ul>
          </div>
          <div>
            <h3 class="font-semibold mb-2">Estado del Sistema</h3>
            <div class="text-sm">
              <div class="flex items-center space-x-2 text-gray-300">
                <div class="w-2 h-2 bg-green-400 rounded-full"></div>
                <span>API: Operacional</span>
              </div>
              <div class="flex items-center space-x-2 text-gray-300 mt-1">
                <div class="w-2 h-2 bg-green-400 rounded-full"></div>
                <span>BD: Conectada</span>
              </div>
            </div>
          </div>
        </div>

        <div class="border-t border-gray-700 mt-6 pt-4 text-center text-gray-400 text-sm">
          <p>&copy; 2025 Shipment XML Generator v5.0 - Versión Web</p>
        </div>
      </div>
    </footer>

    <!-- Toast Notifications -->
    <div v-if="notifications.length > 0" class="fixed top-4 right-4 space-y-2 z-50">
      <div
        v-for="notification in notifications"
        :key="notification.id"
        class="bg-white rounded-lg shadow-lg border p-4 max-w-md fade-in"
        :class="getNotificationClass(notification.type)"
      >
        <div class="flex items-start space-x-3">
          <component
            :is="getNotificationIcon(notification.type)"
            class="w-5 h-5 flex-shrink-0 mt-0.5"
            :class="getNotificationIconColor(notification.type)"
          />
          <div class="flex-grow">
            <p class="font-medium text-sm">{{ notification.title }}</p>
            <p v-if="notification.message" class="text-xs text-gray-600 mt-1">
              {{ notification.message }}
            </p>
          </div>
          <button
            @click="removeNotification(notification.id)"
            class="text-gray-400 hover:text-gray-600 transition-colors"
          >
            <XMarkIcon class="w-4 h-4" />
          </button>
        </div>
      </div>
    </div>
  </div>
</template>



<style scoped>
/* Estilos específicos del componente */
.fade-in {
  animation: fadeIn 0.5s ease-in-out;
}

@keyframes fadeIn {
  from {
    opacity: 0;
    transform: translateY(20px);
  }
  to {
    opacity: 1;
    transform: translateY(0);
  }
}

/* Animación del spinner */
.animate-spin {
  animation: spin 1s linear infinite;
}

@keyframes spin {
  from {
    transform: rotate(0deg);
  }
  to {
    transform: rotate(360deg);
  }
}
</style>
