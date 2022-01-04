<?php
declare(strict_types=1);

namespace App\Controller;

/**
 * IndicadoresFundos Controller
 *
 * @property \App\Model\Table\IndicadoresFundosTable $IndicadoresFundos
 * @method \App\Model\Entity\IndicadoresFundo[]|\Cake\Datasource\ResultSetInterface paginate($object = null, array $settings = [])
 */
class IndicadoresFundosController extends AppController
{
    /**
     * Index method
     *
     * @return \Cake\Http\Response|null|void Renders view
     */
    public function index()
    {
        $this->paginate = [
            'contain' => ['CnpjFundos', 'TipoBenchmarks'],
        ];
        $indicadoresFundos = $this->paginate($this->IndicadoresFundos);

        $this->set(compact('indicadoresFundos'));
    }

    /**
     * View method
     *
     * @param string|null $id Indicadores Fundo id.
     * @return \Cake\Http\Response|null|void Renders view
     * @throws \Cake\Datasource\Exception\RecordNotFoundException When record not found.
     */
    public function view($id = null)
    {
        $indicadoresFundo = $this->IndicadoresFundos->get($id, [
            'contain' => ['CnpjFundos', 'TipoBenchmarks'],
        ]);

        $this->set(compact('indicadoresFundo'));
    }

    /**
     * Add method
     *
     * @return \Cake\Http\Response|null|void Redirects on successful add, renders view otherwise.
     */
    public function add()
    {
        $indicadoresFundo = $this->IndicadoresFundos->newEmptyEntity();
        if ($this->request->is('post')) {
            $indicadoresFundo = $this->IndicadoresFundos->patchEntity($indicadoresFundo, $this->request->getData());
            if ($this->IndicadoresFundos->save($indicadoresFundo)) {
                $this->Flash->success(__('The indicadores fundo has been saved.'));

                return $this->redirect(['action' => 'index']);
            }
            $this->Flash->error(__('The indicadores fundo could not be saved. Please, try again.'));
        }
        $cnpjFundos = $this->IndicadoresFundos->CnpjFundos->find('list', ['limit' => 200]);
        $tipoBenchmarks = $this->IndicadoresFundos->TipoBenchmarks->find('list', ['limit' => 200]);
        $this->set(compact('indicadoresFundo', 'cnpjFundos', 'tipoBenchmarks'));
    }

    /**
     * Edit method
     *
     * @param string|null $id Indicadores Fundo id.
     * @return \Cake\Http\Response|null|void Redirects on successful edit, renders view otherwise.
     * @throws \Cake\Datasource\Exception\RecordNotFoundException When record not found.
     */
    public function edit($id = null)
    {
        $indicadoresFundo = $this->IndicadoresFundos->get($id, [
            'contain' => [],
        ]);
        if ($this->request->is(['patch', 'post', 'put'])) {
            $indicadoresFundo = $this->IndicadoresFundos->patchEntity($indicadoresFundo, $this->request->getData());
            if ($this->IndicadoresFundos->save($indicadoresFundo)) {
                $this->Flash->success(__('The indicadores fundo has been saved.'));

                return $this->redirect(['action' => 'index']);
            }
            $this->Flash->error(__('The indicadores fundo could not be saved. Please, try again.'));
        }
        $cnpjFundos = $this->IndicadoresFundos->CnpjFundos->find('list', ['limit' => 200]);
        $tipoBenchmarks = $this->IndicadoresFundos->TipoBenchmarks->find('list', ['limit' => 200]);
        $this->set(compact('indicadoresFundo', 'cnpjFundos', 'tipoBenchmarks'));
    }

    /**
     * Delete method
     *
     * @param string|null $id Indicadores Fundo id.
     * @return \Cake\Http\Response|null|void Redirects to index.
     * @throws \Cake\Datasource\Exception\RecordNotFoundException When record not found.
     */
    public function delete($id = null)
    {
        $this->request->allowMethod(['post', 'delete']);
        $indicadoresFundo = $this->IndicadoresFundos->get($id);
        if ($this->IndicadoresFundos->delete($indicadoresFundo)) {
            $this->Flash->success(__('The indicadores fundo has been deleted.'));
        } else {
            $this->Flash->error(__('The indicadores fundo could not be deleted. Please, try again.'));
        }

        return $this->redirect(['action' => 'index']);
    }
}
