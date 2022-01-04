<?php
declare(strict_types=1);

namespace App\Controller;

/**
 * RelBenchmarksClasseFundos Controller
 *
 * @property \App\Model\Table\RelBenchmarksClasseFundosTable $RelBenchmarksClasseFundos
 * @method \App\Model\Entity\RelBenchmarksClasseFundo[]|\Cake\Datasource\ResultSetInterface paginate($object = null, array $settings = [])
 */
class RelBenchmarksClasseFundosController extends AppController
{
    /**
     * Index method
     *
     * @return \Cake\Http\Response|null|void Renders view
     */
    public function index()
    {
        $this->paginate = [
            'contain' => ['TipoBenchmarks', 'TipoClasseFundos'],
        ];
        $relBenchmarksClasseFundos = $this->paginate($this->RelBenchmarksClasseFundos);

        $this->set(compact('relBenchmarksClasseFundos'));
    }

    /**
     * View method
     *
     * @param string|null $id Rel Benchmarks Classe Fundo id.
     * @return \Cake\Http\Response|null|void Renders view
     * @throws \Cake\Datasource\Exception\RecordNotFoundException When record not found.
     */
    public function view($id = null)
    {
        $relBenchmarksClasseFundo = $this->RelBenchmarksClasseFundos->get($id, [
            'contain' => ['TipoBenchmarks', 'TipoClasseFundos'],
        ]);

        $this->set(compact('relBenchmarksClasseFundo'));
    }

    /**
     * Add method
     *
     * @return \Cake\Http\Response|null|void Redirects on successful add, renders view otherwise.
     */
    public function add()
    {
        $relBenchmarksClasseFundo = $this->RelBenchmarksClasseFundos->newEmptyEntity();
        if ($this->request->is('post')) {
            $relBenchmarksClasseFundo = $this->RelBenchmarksClasseFundos->patchEntity($relBenchmarksClasseFundo, $this->request->getData());
            if ($this->RelBenchmarksClasseFundos->save($relBenchmarksClasseFundo)) {
                $this->Flash->success(__('The rel benchmarks classe fundo has been saved.'));

                return $this->redirect(['action' => 'index']);
            }
            $this->Flash->error(__('The rel benchmarks classe fundo could not be saved. Please, try again.'));
        }
        $tipoBenchmarks = $this->RelBenchmarksClasseFundos->TipoBenchmarks->find('list', ['limit' => 200]);
        $tipoClasseFundos = $this->RelBenchmarksClasseFundos->TipoClasseFundos->find('list', ['limit' => 200]);
        $this->set(compact('relBenchmarksClasseFundo', 'tipoBenchmarks', 'tipoClasseFundos'));
    }

    /**
     * Edit method
     *
     * @param string|null $id Rel Benchmarks Classe Fundo id.
     * @return \Cake\Http\Response|null|void Redirects on successful edit, renders view otherwise.
     * @throws \Cake\Datasource\Exception\RecordNotFoundException When record not found.
     */
    public function edit($id = null)
    {
        $relBenchmarksClasseFundo = $this->RelBenchmarksClasseFundos->get($id, [
            'contain' => [],
        ]);
        if ($this->request->is(['patch', 'post', 'put'])) {
            $relBenchmarksClasseFundo = $this->RelBenchmarksClasseFundos->patchEntity($relBenchmarksClasseFundo, $this->request->getData());
            if ($this->RelBenchmarksClasseFundos->save($relBenchmarksClasseFundo)) {
                $this->Flash->success(__('The rel benchmarks classe fundo has been saved.'));

                return $this->redirect(['action' => 'index']);
            }
            $this->Flash->error(__('The rel benchmarks classe fundo could not be saved. Please, try again.'));
        }
        $tipoBenchmarks = $this->RelBenchmarksClasseFundos->TipoBenchmarks->find('list', ['limit' => 200]);
        $tipoClasseFundos = $this->RelBenchmarksClasseFundos->TipoClasseFundos->find('list', ['limit' => 200]);
        $this->set(compact('relBenchmarksClasseFundo', 'tipoBenchmarks', 'tipoClasseFundos'));
    }

    /**
     * Delete method
     *
     * @param string|null $id Rel Benchmarks Classe Fundo id.
     * @return \Cake\Http\Response|null|void Redirects to index.
     * @throws \Cake\Datasource\Exception\RecordNotFoundException When record not found.
     */
    public function delete($id = null)
    {
        $this->request->allowMethod(['post', 'delete']);
        $relBenchmarksClasseFundo = $this->RelBenchmarksClasseFundos->get($id);
        if ($this->RelBenchmarksClasseFundos->delete($relBenchmarksClasseFundo)) {
            $this->Flash->success(__('The rel benchmarks classe fundo has been deleted.'));
        } else {
            $this->Flash->error(__('The rel benchmarks classe fundo could not be deleted. Please, try again.'));
        }

        return $this->redirect(['action' => 'index']);
    }
}
