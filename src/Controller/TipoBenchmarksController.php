<?php
declare(strict_types=1);

namespace App\Controller;

/**
 * TipoBenchmarks Controller
 *
 * @property \App\Model\Table\TipoBenchmarksTable $TipoBenchmarks
 * @method \App\Model\Entity\TipoBenchmark[]|\Cake\Datasource\ResultSetInterface paginate($object = null, array $settings = [])
 */
class TipoBenchmarksController extends AppController
{
    /**
     * Index method
     *
     * @return \Cake\Http\Response|null|void Renders view
     */
    public function index()
    {
        $tipoBenchmarks = $this->paginate($this->TipoBenchmarks);

        $this->set(compact('tipoBenchmarks'));
    }

    /**
     * View method
     *
     * @param string|null $id Tipo Benchmark id.
     * @return \Cake\Http\Response|null|void Renders view
     * @throws \Cake\Datasource\Exception\RecordNotFoundException When record not found.
     */
    public function view($id = null)
    {
        $tipoBenchmark = $this->TipoBenchmarks->get($id, [
            'contain' => ['IndicadoresFundos', 'RelBenchmarksClasseFundos'],
        ]);

        $this->set(compact('tipoBenchmark'));
    }

    /**
     * Add method
     *
     * @return \Cake\Http\Response|null|void Redirects on successful add, renders view otherwise.
     */
    public function add()
    {
        $tipoBenchmark = $this->TipoBenchmarks->newEmptyEntity();
        if ($this->request->is('post')) {
            $tipoBenchmark = $this->TipoBenchmarks->patchEntity($tipoBenchmark, $this->request->getData());
            if ($this->TipoBenchmarks->save($tipoBenchmark)) {
                $this->Flash->success(__('The tipo benchmark has been saved.'));

                return $this->redirect(['action' => 'index']);
            }
            $this->Flash->error(__('The tipo benchmark could not be saved. Please, try again.'));
        }
        $this->set(compact('tipoBenchmark'));
    }

    /**
     * Edit method
     *
     * @param string|null $id Tipo Benchmark id.
     * @return \Cake\Http\Response|null|void Redirects on successful edit, renders view otherwise.
     * @throws \Cake\Datasource\Exception\RecordNotFoundException When record not found.
     */
    public function edit($id = null)
    {
        $tipoBenchmark = $this->TipoBenchmarks->get($id, [
            'contain' => [],
        ]);
        if ($this->request->is(['patch', 'post', 'put'])) {
            $tipoBenchmark = $this->TipoBenchmarks->patchEntity($tipoBenchmark, $this->request->getData());
            if ($this->TipoBenchmarks->save($tipoBenchmark)) {
                $this->Flash->success(__('The tipo benchmark has been saved.'));

                return $this->redirect(['action' => 'index']);
            }
            $this->Flash->error(__('The tipo benchmark could not be saved. Please, try again.'));
        }
        $this->set(compact('tipoBenchmark'));
    }

    /**
     * Delete method
     *
     * @param string|null $id Tipo Benchmark id.
     * @return \Cake\Http\Response|null|void Redirects to index.
     * @throws \Cake\Datasource\Exception\RecordNotFoundException When record not found.
     */
    public function delete($id = null)
    {
        $this->request->allowMethod(['post', 'delete']);
        $tipoBenchmark = $this->TipoBenchmarks->get($id);
        if ($this->TipoBenchmarks->delete($tipoBenchmark)) {
            $this->Flash->success(__('The tipo benchmark has been deleted.'));
        } else {
            $this->Flash->error(__('The tipo benchmark could not be deleted. Please, try again.'));
        }

        return $this->redirect(['action' => 'index']);
    }
}
