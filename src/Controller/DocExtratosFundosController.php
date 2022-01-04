<?php
declare(strict_types=1);

namespace App\Controller;

/**
 * DocExtratosFundos Controller
 *
 * @property \App\Model\Table\DocExtratosFundosTable $DocExtratosFundos
 * @method \App\Model\Entity\DocExtratosFundo[]|\Cake\Datasource\ResultSetInterface paginate($object = null, array $settings = [])
 */
class DocExtratosFundosController extends AppController
{
    /**
     * Index method
     *
     * @return \Cake\Http\Response|null|void Renders view
     */
    public function index()
    {
        $this->paginate = [
            'contain' => ['CnpjFundos', 'TipoAnbimaClasses'],
        ];
        $docExtratosFundos = $this->paginate($this->DocExtratosFundos);

        $this->set(compact('docExtratosFundos'));
    }

    /**
     * View method
     *
     * @param string|null $id Doc Extratos Fundo id.
     * @return \Cake\Http\Response|null|void Renders view
     * @throws \Cake\Datasource\Exception\RecordNotFoundException When record not found.
     */
    public function view($id = null)
    {
        $docExtratosFundo = $this->DocExtratosFundos->get($id, [
            'contain' => ['CnpjFundos', 'TipoAnbimaClasses'],
        ]);

        $this->set(compact('docExtratosFundo'));
    }

    /**
     * Add method
     *
     * @return \Cake\Http\Response|null|void Redirects on successful add, renders view otherwise.
     */
    public function add()
    {
        $docExtratosFundo = $this->DocExtratosFundos->newEmptyEntity();
        if ($this->request->is('post')) {
            $docExtratosFundo = $this->DocExtratosFundos->patchEntity($docExtratosFundo, $this->request->getData());
            if ($this->DocExtratosFundos->save($docExtratosFundo)) {
                $this->Flash->success(__('The doc extratos fundo has been saved.'));

                return $this->redirect(['action' => 'index']);
            }
            $this->Flash->error(__('The doc extratos fundo could not be saved. Please, try again.'));
        }
        $cnpjFundos = $this->DocExtratosFundos->CnpjFundos->find('list', ['limit' => 200]);
        $tipoAnbimaClasses = $this->DocExtratosFundos->TipoAnbimaClasses->find('list', ['limit' => 200]);
        $this->set(compact('docExtratosFundo', 'cnpjFundos', 'tipoAnbimaClasses'));
    }

    /**
     * Edit method
     *
     * @param string|null $id Doc Extratos Fundo id.
     * @return \Cake\Http\Response|null|void Redirects on successful edit, renders view otherwise.
     * @throws \Cake\Datasource\Exception\RecordNotFoundException When record not found.
     */
    public function edit($id = null)
    {
        $docExtratosFundo = $this->DocExtratosFundos->get($id, [
            'contain' => [],
        ]);
        if ($this->request->is(['patch', 'post', 'put'])) {
            $docExtratosFundo = $this->DocExtratosFundos->patchEntity($docExtratosFundo, $this->request->getData());
            if ($this->DocExtratosFundos->save($docExtratosFundo)) {
                $this->Flash->success(__('The doc extratos fundo has been saved.'));

                return $this->redirect(['action' => 'index']);
            }
            $this->Flash->error(__('The doc extratos fundo could not be saved. Please, try again.'));
        }
        $cnpjFundos = $this->DocExtratosFundos->CnpjFundos->find('list', ['limit' => 200]);
        $tipoAnbimaClasses = $this->DocExtratosFundos->TipoAnbimaClasses->find('list', ['limit' => 200]);
        $this->set(compact('docExtratosFundo', 'cnpjFundos', 'tipoAnbimaClasses'));
    }

    /**
     * Delete method
     *
     * @param string|null $id Doc Extratos Fundo id.
     * @return \Cake\Http\Response|null|void Redirects to index.
     * @throws \Cake\Datasource\Exception\RecordNotFoundException When record not found.
     */
    public function delete($id = null)
    {
        $this->request->allowMethod(['post', 'delete']);
        $docExtratosFundo = $this->DocExtratosFundos->get($id);
        if ($this->DocExtratosFundos->delete($docExtratosFundo)) {
            $this->Flash->success(__('The doc extratos fundo has been deleted.'));
        } else {
            $this->Flash->error(__('The doc extratos fundo could not be deleted. Please, try again.'));
        }

        return $this->redirect(['action' => 'index']);
    }
}
