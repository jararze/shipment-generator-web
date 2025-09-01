<script>
import { reactive, watch } from 'vue'
import { Cog6ToothIcon, InformationCircleIcon } from '@heroicons/vue/24/outline'

export default {
  name: 'ProcessingOptions',
  components: {
    Cog6ToothIcon,
    InformationCircleIcon,
  },
  props: {
    options: {
      type: Object,
      default: () => ({
        usePlantaAsOrigen: false,
        skipPlacas: false,
      }),
    },
  },
  emits: ['options-changed'],
  setup(props, { emit }) {
    const localOptions = reactive({ ...props.options })

    // Observar cambios y emitir eventos
    watch(
      localOptions,
      (newOptions) => {
        emit('options-changed', { ...newOptions })
      },
      { deep: true },
    )

    return {
      localOptions,
    }
  },
}
</script>

<template>
  <div class="bg-white rounded-lg shadow-md p-6 mb-6">
    <h2 class="text-xl font-semibold mb-4 flex items-center">
      <Cog6ToothIcon class="w-5 h-5 mr-2" />
      Opciones de Procesamiento
    </h2>

    <div class="space-y-6">
      <div class="grid md:grid-cols-2 gap-6">
        <!-- Opción 1 -->
        <div class="bg-gray-50 rounded-lg p-4">
          <label class="flex items-start space-x-3 cursor-pointer">
            <input
              v-model="localOptions.usePlantaAsOrigen"
              type="checkbox"
              class="w-5 h-5 text-blue-600 rounded focus:ring-blue-500 mt-0.5"
            />
            <div>
              <span class="font-medium text-gray-900">Usar 'Cod Planta' como origen</span>
              <p class="text-sm text-gray-600 mt-1">
                Utiliza la columna 'Cod Planta' en lugar de 'Cód. Origen' para determinar el punto
                de partida del envío.
              </p>
              <span class="inline-block mt-2 px-2 py-1 bg-blue-100 text-blue-700 text-xs rounded">
                --from-planta
              </span>
            </div>
          </label>
        </div>

        <!-- Opción 2 -->
        <div class="bg-gray-50 rounded-lg p-4">
          <label class="flex items-start space-x-3 cursor-pointer">
            <input
              v-model="localOptions.skipPlacas"
              type="checkbox"
              class="w-5 h-5 text-blue-600 rounded focus:ring-blue-500 mt-0.5"
            />
            <div>
              <span class="font-medium text-gray-900">Omitir archivo de placas</span>
              <p class="text-sm text-gray-600 mt-1">
                No generar el archivo de disponibilidad de placas. Solo se creará el XML principal.
              </p>
              <span
                class="inline-block mt-2 px-2 py-1 bg-orange-100 text-orange-700 text-xs rounded"
              >
                --no-placas
              </span>
            </div>
          </label>
        </div>
      </div>

      <!-- Información adicional -->
      <div class="bg-blue-50 border border-blue-200 rounded-lg p-4">
        <div class="flex items-start space-x-3">
          <InformationCircleIcon class="w-5 h-5 text-blue-500 mt-0.5 flex-shrink-0" />
          <div class="text-sm">
            <p class="font-medium text-blue-800 mb-2">Información sobre las opciones:</p>
            <ul class="text-blue-700 space-y-1">
              <li>
                • <strong>Cod Planta:</strong> Útil cuando el origen real está en una columna
                diferente
              </li>
              <li>
                • <strong>Sin placas:</strong> Acelera el procesamiento si no necesitas el archivo
                de disponibilidad
              </li>
              <li>• Ambas opciones son independientes y se pueden combinar</li>
            </ul>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<style scoped></style>
