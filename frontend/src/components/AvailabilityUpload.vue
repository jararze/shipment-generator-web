<script>
import { ref } from 'vue';
import { CloudArrowUpIcon, CheckCircleIcon, XCircleIcon } from '@heroicons/vue/24/outline';

export default {
  name: 'AvailabilityUpload',
  components: { CloudArrowUpIcon, CheckCircleIcon, XCircleIcon },
  emits: ['file-selected', 'file-cleared'],
  setup(props, { emit }) {
    const selectedFile = ref(null);

    const handleFileSelect = (event) => {
      const file = event.target.files[0];
      if (file) {
        selectedFile.value = file;
        emit('file-selected', file);
      }
    };

    const clearFile = () => {
      selectedFile.value = null;
      emit('file-cleared');
      // Limpiar el input
      const fileInput = document.querySelector('#availability-input');
      if (fileInput) fileInput.value = '';
    };

    return { selectedFile, handleFileSelect, clearFile };
  }
};
</script>

<template>
  <div class="bg-white rounded-lg shadow-md p-6 mb-6">
    <h2 class="text-xl font-semibold mb-4 flex items-center">
      <CloudArrowUpIcon class="w-5 h-5 mr-2 text-gray-500" />
      <span>Subir Disponibilidad de Camiones (Opcional)</span>
    </h2>
    <div v-if="!selectedFile" class="text-center">
      <p class="text-sm text-gray-500 mb-4">
        Si tienes un archivo de disponibilidad (.xlsx), súbelo aquí para obtener un reporte de placas más completo.
      </p>

      <input id="availability-input" type="file" accept=".xlsx" @change="handleFileSelect" class="hidden" />

      <input id="availability-input" ref="availabilityInput" type="file" accept=".xlsx" @change="handleFileSelect" class="hidden" />

      <button @click="$refs.availabilityInput?.click()" class="bg-gray-200 text-gray-700 px-6 py-2 rounded-lg hover:bg-gray-300 transition-colors">
        Seleccionar Archivo
      </button>
    </div>
    <div v-else class="bg-teal-50 border border-teal-200 rounded-lg p-4 flex items-center justify-between">
      <div class="flex items-center space-x-3">
        <CheckCircleIcon class="w-6 h-6 text-teal-600" />
        <p class="font-medium text-teal-800">{{ selectedFile.name }}</p>
      </div>
      <button @click="clearFile" title="Eliminar archivo" class="text-red-500 hover:text-red-700">
        <XCircleIcon class="w-6 h-6" />
      </button>
    </div>
  </div>
</template>

<style scoped>

</style>
