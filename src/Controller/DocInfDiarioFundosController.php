<?php
declare(strict_types=1);

namespace App\Controller;

/**
 * DocInfDiarioFundos Controller
 *
 * @property \App\Model\Table\DocInfDiarioFundosTable $DocInfDiarioFundos
 * @method \App\Model\Entity\DocInfDiarioFundo[]|\Cake\Datasource\ResultSetInterface paginate($object = null, array $settings = [])
 */
class DocInfDiarioFundosController extends AppController
{
    /**
     * Index method
     *
     * @return \Cake\Http\Response|null|void Renders view
     */
    public function index()
    {
        $this->paginate = [
            'contain' => ['CnpjFundos'],
        ];
        $docInfDiarioFundos = $this->paginate($this->DocInfDiarioFundos);

        $this->set(compact('docInfDiarioFundos'));
    }

    /**
     * View method
     *
     * @param string|null $id Doc Inf Diario Fundo id.
     * @return \Cake\Http\Response|null|void Renders view
     * @throws \Cake\Datasource\Exception\RecordNotFoundException When record not found.
     */
    public function view($id = null)
    {
        $docInfDiarioFundo = $this->DocInfDiarioFundos->get($id, [
            'contain' => ['CnpjFundos'],
        ]);

        $this->set(compact('docInfDiarioFundo'));
    }

    /**
     * Add method
     *
     * @return \Cake\Http\Response|null|void Redirects on successful add, renders view otherwise.
     */
    public function add()
    {
        $docInfDiarioFundo = $this->DocInfDiarioFundos->newEmptyEntity();
        if ($this->request->is('post')) {
            $docInfDiarioFundo = $this->DocInfDiarioFundos->patchEntity($docInfDiarioFundo, $this->request->getData());
            if ($this->DocInfDiarioFundos->save($docInfDiarioFundo)) {
                $this->Flash->success(__('The doc inf diario fundo has been saved.'));

                return $this->redirect(['action' => 'index']);
            }
            $this->Flash->error(__('The doc inf diario fundo could not be saved. Please, try again.'));
        }
        $cnpjFundos = $this->DocInfDiarioFundos->CnpjFundos->find('list', ['limit' => 200]);
        $this->set(compact('docInfDiarioFundo', 'cnpjFundos'));
    }

    /**
     * Edit method
     *
     * @param string|null $id Doc Inf Diario Fundo id.
     * @return \Cake\Http\Response|null|void Redirects on successful edit, renders view otherwise.
     * @throws \Cake\Datasource\Exception\RecordNotFoundException When record not found.
     */
    public function edit($id = null)
    {
        $docInfDiarioFundo = $this->DocInfDiarioFundos->get($id, [
            'contain' => [],
        ]);
        if ($this->request->is(['patch', 'post', 'put'])) {
            $docInfDiarioFundo = $this->DocInfDiarioFundos->patchEntity($docInfDiarioFundo, $this->request->getData());
            if ($this->DocInfDiarioFundos->save($docInfDiarioFundo)) {
                $this->Flash->success(__('The doc inf diario fundo has been saved.'));

                return $this->redirect(['action' => 'index']);
            }
            $this->Flash->error(__('The doc inf diario fundo could not be saved. Please, try again.'));
        }
        $cnpjFundos = $this->DocInfDiarioFundos->CnpjFundos->find('list', ['limit' => 200]);
        $this->set(compact('docInfDiarioFundo', 'cnpjFundos'));
    }

    /**
     * Delete method
     *
     * @param string|null $id Doc Inf Diario Fundo id.
     * @return \Cake\Http\Response|null|void Redirects to index.
     * @throws \Cake\Datasource\Exception\RecordNotFoundException When record not found.
     */
    public function delete($id = null)
    {
        $this->request->allowMethod(['post', 'delete']);
        $docInfDiarioFundo = $this->DocInfDiarioFundos->get($id);
        if ($this->DocInfDiarioFundos->delete($docInfDiarioFundo)) {
            $this->Flash->success(__('The doc inf diario fundo has been deleted.'));
        } else {
            $this->Flash->error(__('The doc inf diario fundo could not be deleted. Please, try again.'));
        }

        return $this->redirect(['action' => 'index']);
    }
}
