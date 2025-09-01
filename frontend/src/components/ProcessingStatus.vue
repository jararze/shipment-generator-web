<script>
import {
  ClockIcon,
  CheckCircleIcon,
  ExclamationTriangleIcon,
  InformationCircleIcon,
  ChartBarIcon,
  CogIcon,
} from '@heroicons/vue/24/outline'
import { useFileHandling } from '@/composables/useApi'

export default {
  name: 'ProcessingStatus',
  components: {
    ClockIcon,
    CheckCircleIcon,
    ExclamationTriangleIcon,
    InformationCircleIcon,
    ChartBarIcon,
    CogIcon,
  },
  props: {
    job: {
      type: Object,
      default: null,
    },
  },
  setup(props) {
    const { formatFileSize } = useFileHandling()

    const getStatusIcon = () => {
      if (!props.job) return 'ClockIcon'

      switch (props.job.status) {
        case 'processing':
          return 'CogIcon'
        case 'completed':
          return 'CheckCircleIcon'
        case 'error':
          return 'ExclamationTriangleIcon'
        default:
          return 'ClockIcon'
      }
    }

    const getStatusColor = () => {
      if (!props.job) return 'text-gray-500'

      switch (props.job.status) {
        case 'processing':
          return 'text-blue-500 animate-spin'
        case 'completed':
          return 'text-green-500'
        case 'error':
          return 'text-red-500'
        default:
          return 'text-gray-500'
      }
    }

    const getStatusText = () => {
      if (!props.job) return 'Pendiente'

      switch (props.job.status) {
        case 'processing':
          return 'Procesando'
        case 'completed':
          return 'Completado'
        case 'error':
          return 'Error'
        default:
          return 'Pendiente'
      }
    }

    const getStatusBadgeClass = () => {
      if (!props.job) return 'bg-gray-100 text-gray-700'

      switch (props.job.status) {
        case 'processing':
          return 'bg-blue-100 text-blue-700 pulse-green'
        case 'completed':
          return 'bg-green-100 text-green-700'
        case 'error':
          return 'bg-red-100 text-red-700'
        default:
          return 'bg-gray-100 text-gray-700'
      }
    }

    const getProgressBarClass = () => {
      if (!props.job) return 'bg-gray-400'

      switch (props.job.status) {
        case 'processing':
          return 'bg-blue-600'
        case 'completed':
          return 'bg-green-600'
        case 'error':
          return 'bg-red-600'
        default:
          return 'bg-gray-400'
      }
    }

    const formatDate = (dateString) => {
      try {
        return new Date(dateString).toLocaleString('es-ES', {
          year: 'numeric',
          month: '2-digit',
          day: '2-digit',
          hour: '2-digit',
          minute: '2-digit',
        })
      } catch {
        return dateString
      }
    }

    return {
      formatFileSize,
      getStatusIcon,
      getStatusColor,
      getStatusText,
      getStatusBadgeClass,
      getProgressBarClass,
      formatDate,
    }
  },
}
</script>

<template>
  <div v-if="job" class="bg-white rounded-lg shadow-md p-6 mb-6 fade-in">
    <div class="flex items-center space-x-3 mb-6">
      <component :is="getStatusIcon()" class="w-6 h-6" :class="getStatusColor()" />
      <h2 class="text-xl font-semibold">Estado del Procesamiento</h2>
      <span class="px-3 py-1 rounded-full text-sm font-medium" :class="getStatusBadgeClass()">
        {{ getStatusText() }}
      </span>
    </div>

    <!-- Información del archivo -->
    <div class="bg-gray-50 rounded-lg p-4 mb-6">
      <div class="grid md:grid-cols-3 gap-4 text-sm">
        <div>
          <span class="font-medium text-gray-700">Archivo:</span>
          <p class="text-gray-900">{{ job.filename || 'Sin nombre' }}</p>
        </div>
        <div v-if="job.fileSize">
          <span class="font-medium text-gray-700">Tamaño:</span>
          <p class="text-gray-900">{{ formatFileSize(job.fileSize) }}</p>
        </div>
        <div v-if="job.started_at">
          <span class="font-medium text-gray-700">Iniciado:</span>
          <p class="text-gray-900">{{ formatDate(job.started_at) }}</p>
        </div>
      </div>
    </div>

    <!-- Barra de progreso -->
    <div class="mb-6">
      <div class="flex justify-between items-center mb-2">
        <span class="text-sm font-medium text-gray-700">Progreso</span>
        <span class="text-sm font-medium text-gray-700">{{ job.progress }}%</span>
      </div>
      <div class="w-full bg-gray-200 rounded-full h-3 relative overflow-hidden">
        <div
          class="h-3 rounded-full transition-all duration-500 ease-out relative"
          :class="getProgressBarClass()"
          :style="{ width: `${job.progress}%` }"
        >
          <!-- Efecto de animación para progreso activo -->
          <div
            v-if="job.status === 'processing'"
            class="absolute inset-0 bg-gradient-to-r from-transparent via-white to-transparent opacity-30 animate-pulse"
          ></div>
        </div>
      </div>
    </div>

    <!-- Mensaje de estado -->
    <div class="bg-gray-50 rounded-lg p-4 mb-6">
      <div class="flex items-start space-x-3">
        <InformationCircleIcon class="w-5 h-5 text-blue-500 mt-0.5 flex-shrink-0" />
        <p class="text-gray-700 leading-relaxed">{{ job.message }}</p>
      </div>
    </div>

    <!-- Mostrar error si existe -->
    <div v-if="job.error" class="bg-red-50 border border-red-200 rounded-lg p-4 mb-6">
      <div class="flex items-start space-x-3">
        <ExclamationTriangleIcon class="w-5 h-5 text-red-500 mt-0.5 flex-shrink-0" />
        <div>
          <p class="font-medium text-red-800">Error de Procesamiento:</p>
          <p class="text-red-700 text-sm mt-1 font-mono bg-red-100 p-2 rounded">
            {{ job.error }}
          </p>
        </div>
      </div>
    </div>

    <!-- Estadísticas de validación -->
    <div
      v-if="job.validation_stats && job.status === 'completed'"
      class="bg-blue-50 border border-blue-200 rounded-lg p-4"
    >
      <h3 class="font-medium text-blue-800 mb-3 flex items-center">
        <ChartBarIcon class="w-4 h-4 mr-2" />
        Estadísticas de Procesamiento
      </h3>

      <div class="grid grid-cols-2 md:grid-cols-4 gap-4">
        <div class="bg-white rounded-lg p-3 text-center">
          <div class="text-2xl font-bold text-blue-600">
            {{ job.validation_stats.total_records || 0 }}
          </div>
          <div class="text-xs text-blue-700">Registros Totales</div>
        </div>
        <div class="bg-white rounded-lg p-3 text-center">
          <div class="text-2xl font-bold text-green-600">
            {{ job.validation_stats.database_queries || 0 }}
          </div>
          <div class="text-xs text-green-700">Consultas BD</div>
        </div>
        <div class="bg-white rounded-lg p-3 text-center">
          <div class="text-2xl font-bold text-purple-600">
            {{ job.validation_stats.header_records || 0 }}
          </div>
          <div class="text-xs text-purple-700">Headers</div>
        </div>
        <div class="bg-white rounded-lg p-3 text-center">
          <div class="text-2xl font-bold text-orange-600">
            {{ job.validation_stats.detail_records || 0 }}
          </div>
          <div class="text-xs text-orange-700">Details</div>
        </div>
      </div>
    </div>
  </div>
</template>

<style scoped></style>
