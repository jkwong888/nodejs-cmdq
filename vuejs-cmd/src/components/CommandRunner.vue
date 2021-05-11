<template>
  <v-container>
    <v-row>
      <v-select
      :items="agents"
      label="Agents"
      v-model="selectedAgent"
      />
    </v-row>

    <v-row>
      <v-btn
      elevation="2"
      v-on:click="sendCmd"
      :disabled="selectedAgent == ''"
    >Run Command</v-btn>
    </v-row>
    <v-row>
      <v-data-table
        id="table"
        :headers="headers"
        :items="items"
        class="elevation-1"
        :loading="queryLoading"
      ></v-data-table>
    </v-row>
  </v-container>
</template>

<script>
  export default {
    name: 'CommandRunner',

    data () {
      return {
        interval: null,
        items: [],
        agents: [],
        selectedAgent: '',
        queryLoading: false,
        headers: [
          {
            text: 'ID',
            align: 'start',
            sortable: false,
            value: 'i',
          },
          { text: 'Name', value: 'name' },
          { text: 'Email', value: 'email' },
          { text: 'Quantity', value: 'quantity' },
        ],
      }
    },

    mounted() {
      this.getAgents();
    },

    methods: {
      getAgents() {
        this.$axios.get("/api/agent").then((response) => {
          //console.log(response);
          if (response.status == 200) {
            this.agents = response.data;
            this.selectedAgent = this.agents[0];
          }
        });
      },

      pollResults(url) {
        this.$axios.get(url).then((response) => {
          //console.log(response);
          if (response.status == 200) {
            this.items = response.data;
            clearInterval(this.interval);
            this.queryLoading = false;
          }
        });
      },

      sendCmd() {
        clearInterval(this.interval);
        var ref = this;

        this.queryLoading = true;

        this.$axios.post("/api/cmd", {
          agent: this.selectedAgent,
        })
          .then(response => {
            //console.log(response.headers.location);
            this.interval = setInterval(function() {
              ref.pollResults(response.headers.location);
            }, 250);
          })
      },

    },

    beforeDestroy: function() {
      clearInterval(this.interval);
    }

  }
</script>
