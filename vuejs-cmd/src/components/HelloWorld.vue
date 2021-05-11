<template>
  <v-container>
    <v-row>
      <v-btn
      elevation="2"
      v-on:click="sendCmd"
    >Run Command</v-btn>
    </v-row>
    <v-row>
      <v-data-table
        :headers="headers"
        :items="desserts"
        class="elevation-1"
      ></v-data-table>
    </v-row>
  </v-container>
</template>

<script>
  export default {
    name: 'HelloWorld',

    data () {
      return {
        headers: [
          {
            text: 'ID',
            align: 'start',
            sortable: false,
            value: 'name',
          },
          { text: 'Name', value: 'name' },
          { text: 'Email', value: 'email' },
          { text: 'Quantity', value: 'quantity' },
        ],
      }
    },
    methods: {
      sendCmd: function() {

      }
    }

  }
</script>
